<?php

namespace App;

use App\Commands\Visit;
use RuntimeException;

final class Parser
{
    private const DOMAIN_LENGTH = 19;
    private const FIRST_SLUG_OFFSET = 25;
    private const DATE_OFFSET_FROM_EOL = 25;
    private const COMMA_OFFSET_FROM_EOL = 26;
    private const DAY_RANGE = 4096;
    private const READ_SIZE = 1_048_576;
    private const DEFAULT_WORKERS = 9;
    private const FORK_THRESHOLD = 67_108_864;

    private static ?array $uris = null;
    private static ?array $paths = null;
    private static ?array $encodedPaths = null;
    private static ?array $byteChars = null;
    private static ?array $dateCache = null;
    private static ?\Closure $cachedChunkProcessor = null;
    private static ?\Closure $uncachedChunkProcessor = null;
    private static ?\Closure $cachedByteChunkProcessor = null;
    private static ?\Closure $uncachedByteChunkProcessor = null;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        if ($fileSize === false) {
            throw new RuntimeException("Unable to read {$inputPath}");
        }

        self::bootstrap();
        $workerCount = $this->resolveWorkerCount($fileSize);
        $useByteCounts = $this->shouldUseByteCounts($workerCount);

        if ($workerCount === 1 || ! function_exists('pcntl_fork')) {
            [$counts, $firstSeen] = $this->parseRange($inputPath, 0, $fileSize, $useByteCounts);
        } else {
            [$counts, $firstSeen] = $this->parseInParallel(
                $inputPath,
                $outputPath,
                $fileSize,
                $workerCount,
                $useByteCounts,
            );
        }

        file_put_contents(
            $outputPath,
            $this->buildOutput(
                is_string($counts) ? array_values(unpack($useByteCounts ? 'C*' : 'v*', $counts)) : $counts,
                $firstSeen,
            ),
        );
    }

    private static function bootstrap(): void
    {
        if (self::$uris !== null) {
            return;
        }

        self::$uris = [];
        self::$paths = [];
        self::$encodedPaths = [];
        self::$byteChars = [];
        self::$dateCache = [];

        foreach (Visit::all() as $index => $visit) {
            $uri = $visit->uri;
            self::$uris[$index] = $uri;
            self::$paths[$index] = substr($uri, self::DOMAIN_LENGTH);
            self::$encodedPaths[$index] = json_encode(self::$paths[$index], JSON_THROW_ON_ERROR);
        }

        for ($byte = 0; $byte < 256; $byte++) {
            self::$byteChars[$byte] = chr($byte);
        }

        self::$cachedChunkProcessor = self::buildChunkProcessor(useDateCache: true, useByteCounts: false);
        self::$uncachedChunkProcessor = self::buildChunkProcessor(useDateCache: false, useByteCounts: false);
        self::$cachedByteChunkProcessor = self::buildChunkProcessor(useDateCache: true, useByteCounts: true);
        self::$uncachedByteChunkProcessor = self::buildChunkProcessor(useDateCache: false, useByteCounts: true);
    }

    private static function buildChunkProcessor(bool $useDateCache, bool $useByteCounts): \Closure
    {
        $groups = [];

        foreach (self::$uris as $index => $uri) {
            $groups[strlen($uri)][$index] = $uri;
        }

        ksort($groups);

        $code = '$byteChars = self::$byteChars;' . "\n";
        $code .= 'return static function(' . "\n";
        $code .= '    string $buffer,' . "\n";
        $code .= '    int $limit,' . "\n";
        $code .= '    string &$counts,' . "\n";
        $code .= '    array &$firstSeen,' . "\n";
        $code .= '    array &$parsedDateCache,' . "\n";
        $code .= '    int &$remainingFirstSeen,' . "\n";
        $code .= '    int &$sequence,' . "\n";
        $code .= '    int $uriCount,' . "\n";
        $code .= ') use ($byteChars): void {' . "\n";
        $code .= '    $lineStart = 0;' . "\n";
        $code .= '    $commaLimit = $limit - 26;' . "\n";
        $code .= '    while (($comma = strpos($buffer, ",", $lineStart)) !== false && $comma < $commaLimit) {' . "\n";
        $code .= '        $uriLength = $comma - $lineStart;' . "\n";
        $code .= '        switch ($uriLength) {' . "\n";

        foreach ($groups as $uriLength => $candidates) {
            $code .= "case {$uriLength}:\n";
            $code .= self::buildChunkProcessorTree($candidates, 4);
        }

        $code .= "default:\n";
        $code .= "throw new \\RuntimeException('Unknown URI length');\n";
        $code .= "        }\n";
        $code .= "        resolved_uri:\n";
        $code .= "        if (\$remainingFirstSeen !== 0 && \$firstSeen[\$uriIndex] === -1) {\n";
        $code .= "            \$firstSeen[\$uriIndex] = \$sequence;\n";
        $code .= "            \$remainingFirstSeen--;\n";
        $code .= "        }\n";
        $code .= "        \$dateStart = \$comma + 1;\n";

        if ($useDateCache) {
            $code .= "        \$dateKey = substr(\$buffer, \$dateStart, 10);\n";
            $code .= "        \$dayCode = \$parsedDateCache[\$dateKey] ??= (\n";
            $code .= "            (((ord(\$buffer[\$dateStart + 2]) * 10 + ord(\$buffer[\$dateStart + 3])) - 548) << 9)\n";
            $code .= "            | (((ord(\$buffer[\$dateStart + 5]) * 10 + ord(\$buffer[\$dateStart + 6])) - 528) << 5)\n";
            $code .= "            | ((ord(\$buffer[\$dateStart + 8]) * 10 + ord(\$buffer[\$dateStart + 9])) - 528)\n";
            $code .= "        );\n";
        } else {
            $code .= "        \$dayCode =\n";
            $code .= "            (((ord(\$buffer[\$dateStart + 2]) * 10 + ord(\$buffer[\$dateStart + 3])) - 548) << 9)\n";
            $code .= "            | (((ord(\$buffer[\$dateStart + 5]) * 10 + ord(\$buffer[\$dateStart + 6])) - 528) << 5)\n";
            $code .= "            | ((ord(\$buffer[\$dateStart + 8]) * 10 + ord(\$buffer[\$dateStart + 9])) - 528);\n";
        }

        if ($useByteCounts) {
            $code .= "        \$countOffset = \$dayCode * \$uriCount + \$uriIndex;\n";
            $code .= "        \$byteValue = ord(\$counts[\$countOffset]) + 1;\n";
            $code .= "        if (\$byteValue === 256) {\n";
            $code .= "            throw new \\RuntimeException('Byte counter overflow');\n";
            $code .= "        }\n";
            $code .= "        \$counts[\$countOffset] = \$byteChars[\$byteValue];\n";
        } else {
            $code .= "        \$countOffset = ((\$dayCode * \$uriCount + \$uriIndex) << 1);\n";
            $code .= "        \$lowByte = ord(\$counts[\$countOffset]) + 1;\n";
            $code .= "        if (\$lowByte === 256) {\n";
            $code .= "            \$counts[\$countOffset] = \"\\0\";\n";
            $code .= "            \$countOffset++;\n";
            $code .= "            \$counts[\$countOffset] = \$byteChars[ord(\$counts[\$countOffset]) + 1];\n";
            $code .= "        } else {\n";
            $code .= "            \$counts[\$countOffset] = \$byteChars[\$lowByte];\n";
            $code .= "        }\n";
        }
        $code .= "        \$sequence++;\n";
        $code .= "        \$lineStart = \$comma + 27;\n";
        $code .= "    }\n";
        $code .= "};";

        /** @var \Closure $processor */
        $processor = eval($code);

        return $processor;
    }

    private static function buildChunkProcessorTree(array $candidates, int $indentLevel): string
    {
        if (count($candidates) === 1) {
            $indent = str_repeat('    ', $indentLevel);

            return $indent . '$uriIndex = ' . array_key_first($candidates) . ";\n"
                . $indent . "goto resolved_uri;\n";
        }

        $uriLength = strlen(reset($candidates));
        $bestPosition = null;
        $bestPartitions = null;
        $bestScore = -1;

        for ($position = self::DOMAIN_LENGTH; $position < $uriLength; $position++) {
            $partitions = [];

            foreach ($candidates as $index => $uri) {
                $partitions[$uri[$position]][$index] = $uri;
            }

            $score = count($partitions);

            if ($score > $bestScore) {
                $bestScore = $score;
                $bestPosition = $position;
                $bestPartitions = $partitions;

                if ($score === count($candidates)) {
                    break;
                }
            }
        }

        if ($bestPartitions === null || $bestPosition === null) {
            throw new RuntimeException('Unable to build URI resolver');
        }

        ksort($bestPartitions);
        $indent = str_repeat('    ', $indentLevel);
        $code = $indent . 'switch ($buffer[$lineStart + ' . $bestPosition . "]) {\n";

        foreach ($bestPartitions as $char => $subset) {
            $code .= $indent . 'case ' . var_export($char, true) . ":\n";
            $code .= self::buildChunkProcessorTree($subset, $indentLevel + 1);
        }

        $code .= $indent . "default:\n";
        $code .= $indent . "    throw new \\RuntimeException('Unknown URI');\n";
        $code .= $indent . "}\n";

        return $code;
    }

    private function resolveWorkerCount(int $fileSize): int
    {
        if ($fileSize < self::FORK_THRESHOLD) {
            return 1;
        }

        $override = getenv('PARSER_WORKERS');

        if ($override !== false && ctype_digit($override)) {
            return max(1, (int) $override);
        }

        return self::DEFAULT_WORKERS;
    }

    private function parseInParallel(
        string $inputPath,
        string $outputPath,
        int $fileSize,
        int $workerCount,
        bool $useByteCounts,
    ): array
    {
        $profile = $this->shouldProfile();
        $profileStart = $profile ? microtime(true) : 0.0;
        $ranges = $this->resolveRanges($inputPath, $fileSize, $workerCount);
        $rangesResolvedAt = $profile ? microtime(true) : 0.0;

        if (count($ranges) === 1) {
            return $this->parseRange($inputPath, 0, $fileSize, $useByteCounts);
        }

        if ($this->shouldUseSocketTransport()) {
            return $this->parseInParallelWithSockets(
                $inputPath,
                $fileSize,
                $ranges,
                $useByteCounts,
                $profile,
                $profileStart,
                $rangesResolvedAt,
            );
        }

        $tempDir = dirname($outputPath) . '/.parser-tmp-' . md5($outputPath . ':' . getmypid() . ':' . microtime(true));

        if (! is_dir($tempDir) && ! mkdir($tempDir, 0777, true) && ! is_dir($tempDir)) {
            throw new RuntimeException("Unable to create {$tempDir}");
        }

        $children = [];
        $status = 0;
        $uriCount = count(self::$uris);
        $slotCount = $uriCount * self::DAY_RANGE;
        $useSodiumMerge = ! $useByteCounts
            && function_exists('sodium_add')
            && getenv('PARSER_DISABLE_SODIUM_MERGE') === false;
        $counts = $useSodiumMerge ? null : array_fill(0, $slotCount, 0);
        $firstSeen = array_fill(0, $uriCount, -1);

        foreach ($ranges as $workerIndex => [$start, $end]) {
            $resultPath = "{$tempDir}/worker-{$workerIndex}.bin";

            if (is_file($resultPath)) {
                unlink($resultPath);
            }

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new RuntimeException('Unable to fork worker');
            }

            if ($pid === 0) {
                [$counts, $firstSeen] = $this->parseRange($inputPath, $start, $end, $useByteCounts);
                $this->writeWorkerResult($resultPath, $counts, $firstSeen);
                exit(0);
            }

            $children[$pid] = [$workerIndex, $resultPath];
        }

        while ($children !== []) {
            $pid = pcntl_wait($status);

            if ($pid <= 0) {
                break;
            }

            if (! isset($children[$pid])) {
                continue;
            }

            if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                throw new RuntimeException("Worker {$pid} failed");
            }

            [$workerIndex, $resultPath] = $children[$pid];
            [$workerCounts, $workerSeen] = $this->readWorkerResult(
                $resultPath,
                $slotCount,
                $uriCount,
                $useSodiumMerge || $useByteCounts,
                $useByteCounts,
            );

            if ($useSodiumMerge) {
                if ($counts === null) {
                    $counts = $workerCounts;
                } else {
                    sodium_add($counts, $workerCounts);
                }
            } else {
                if ($useByteCounts) {
                    for ($slot = 0; $slot < $slotCount; $slot++) {
                        $counts[$slot] += ord($workerCounts[$slot]);
                    }
                } else {
                    for ($slot = 0; $slot < $slotCount; $slot++) {
                        $counts[$slot] += $workerCounts[$slot];
                    }
                }
            }

            $workerPrefix = $workerIndex << 32;

            for ($uriIndex = 0; $uriIndex < $uriCount; $uriIndex++) {
                if ($workerSeen[$uriIndex] === -1) {
                    continue;
                }

                $seen = $workerPrefix | $workerSeen[$uriIndex];

                if ($firstSeen[$uriIndex] === -1 || $seen < $firstSeen[$uriIndex]) {
                    $firstSeen[$uriIndex] = $seen;
                }
            }

            if (is_file($resultPath)) {
                unlink($resultPath);
            }

            unset($children[$pid]);
        }
        $workersFinishedAt = $profile ? microtime(true) : 0.0;

        @rmdir($tempDir);

        if ($profile) {
            $mergeFinishedAt = microtime(true);
            fwrite(STDERR, sprintf(
                "profile ranges=%.6f wait=%.6f merge=%.6f total=%.6f\n",
                $rangesResolvedAt - $profileStart,
                $workersFinishedAt - $rangesResolvedAt,
                $mergeFinishedAt - $workersFinishedAt,
                $mergeFinishedAt - $profileStart,
            ));
        }

        return [$counts ?? str_repeat("\0", $slotCount * 2), $firstSeen];
    }

    private function parseInParallelWithSockets(
        string $inputPath,
        int $fileSize,
        array $ranges,
        bool $useByteCounts,
        bool $profile,
        float $profileStart,
        float $rangesResolvedAt,
    ): array
    {
        $uriCount = count(self::$uris);
        $slotCount = $uriCount * self::DAY_RANGE;
        $useSodiumMerge = ! $useByteCounts
            && function_exists('sodium_add')
            && getenv('PARSER_DISABLE_SODIUM_MERGE') === false;
        $counts = $useSodiumMerge ? null : array_fill(0, $slotCount, 0);
        $firstSeen = array_fill(0, $uriCount, -1);
        $children = [];
        $streamMap = [];
        $status = 0;

        foreach ($ranges as $workerIndex => [$start, $end]) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

            if ($pair === false) {
                throw new RuntimeException('Unable to create worker socket pair');
            }

            [$parentStream, $childStream] = $pair;
            stream_set_blocking($parentStream, false);
            $pid = pcntl_fork();

            if ($pid === -1) {
                fclose($parentStream);
                fclose($childStream);
                throw new RuntimeException('Unable to fork worker');
            }

            if ($pid === 0) {
                fclose($parentStream);
                [$workerCounts, $workerSeen] = $this->parseRange($inputPath, $start, $end, $useByteCounts);
                $this->writeWorkerStream($childStream, $workerCounts, $workerSeen);
                fclose($childStream);
                exit(0);
            }

            fclose($childStream);
            $streamId = (int) $parentStream;
            $children[$pid] = [
                'workerIndex' => $workerIndex,
                'stream' => $parentStream,
                'streamId' => $streamId,
                'buffer' => '',
                'streamClosed' => false,
                'exited' => false,
            ];
            $streamMap[$streamId] = $pid;
        }

        while ($children !== []) {
            while (($pid = pcntl_waitpid(-1, $status, WNOHANG)) > 0) {
                if (! isset($children[$pid])) {
                    continue;
                }

                if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                    throw new RuntimeException("Worker {$pid} failed");
                }

                $children[$pid]['exited'] = true;

                if ($children[$pid]['streamClosed']) {
                    unset($children[$pid]);
                }
            }

            $read = [];

            foreach ($children as $child) {
                if (! $child['streamClosed']) {
                    $read[] = $child['stream'];
                }
            }

            if ($read === []) {
                usleep(1000);
                continue;
            }

            $write = null;
            $except = null;
            $selected = stream_select($read, $write, $except, 0, 200000);

            if ($selected === false) {
                throw new RuntimeException('Unable to select worker streams');
            }

            if ($selected === 0) {
                continue;
            }

            foreach ($read as $stream) {
                $streamId = (int) $stream;
                $pid = $streamMap[$streamId] ?? null;

                if ($pid === null || ! isset($children[$pid])) {
                    continue;
                }

                $chunk = fread($stream, 262144);

                if ($chunk !== false && $chunk !== '') {
                    $children[$pid]['buffer'] .= $chunk;
                }

                if (! feof($stream)) {
                    continue;
                }

                fclose($stream);
                unset($streamMap[$streamId]);
                $children[$pid]['streamClosed'] = true;
                [$workerCounts, $workerSeen] = $this->decodeWorkerResult(
                    $children[$pid]['buffer'],
                    $slotCount,
                    $uriCount,
                    $useSodiumMerge || $useByteCounts,
                    $useByteCounts,
                );
                $this->mergeWorkerResult(
                    $counts,
                    $firstSeen,
                    $workerCounts,
                    $workerSeen,
                    $children[$pid]['workerIndex'],
                    $slotCount,
                    $uriCount,
                    $useSodiumMerge,
                    $useByteCounts,
                );
                $children[$pid]['buffer'] = '';

                if ($children[$pid]['exited']) {
                    unset($children[$pid]);
                }
            }
        }

        if ($profile) {
            $finishedAt = microtime(true);
            fwrite(STDERR, sprintf(
                "profile ranges=%.6f wait=%.6f merge=%.6f total=%.6f\n",
                $rangesResolvedAt - $profileStart,
                $finishedAt - $rangesResolvedAt,
                0.0,
                $finishedAt - $profileStart,
            ));
        }

        return [$counts ?? str_repeat("\0", $slotCount * 2), $firstSeen];
    }

    private function resolveRanges(string $inputPath, int $fileSize, int $workerCount): array
    {
        if ($workerCount <= 1) {
            return [[0, $fileSize]];
        }

        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open {$inputPath}");
        }

        $offsets = [0];
        $sliceSize = intdiv($fileSize, $workerCount);

        for ($worker = 1; $worker < $workerCount; $worker++) {
            fseek($handle, $sliceSize * $worker);
            fgets($handle);

            $offset = ftell($handle);

            if ($offset === false) {
                break;
            }

            $offsets[] = $offset;
        }

        $offsets[] = $fileSize;
        fclose($handle);

        $ranges = [];
        $offsetCount = count($offsets) - 1;

        for ($index = 0; $index < $offsetCount; $index++) {
            if ($offsets[$index] < $offsets[$index + 1]) {
                $ranges[] = [$offsets[$index], $offsets[$index + 1]];
            }
        }

        return $ranges;
    }

    private function parseRange(string $inputPath, int $start, int $end, bool $useByteCounts): array
    {
        $chunkProcessor = $useByteCounts
            ? ($this->shouldUseDateCache() ? self::$cachedByteChunkProcessor : self::$uncachedByteChunkProcessor)
            : ($this->shouldUseDateCache() ? self::$cachedChunkProcessor : self::$uncachedChunkProcessor);
        $uriCount = count(self::$uris);
        $readSize = $this->resolveReadSize();
        $slotCount = $uriCount * self::DAY_RANGE;
        $counts = str_repeat("\0", $useByteCounts ? $slotCount : $slotCount * 2);
        $firstSeen = array_fill(0, $uriCount, -1);
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open {$inputPath}");
        }

        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $carry = '';
        $parsedDateCache = [];
        $remainingFirstSeen = $uriCount;
        $sequence = 0;
        $position = $start;

        while ($position < $end) {
            $remaining = $end - $position;
            $buffer = fread($handle, $remaining > $readSize ? $readSize : $remaining);

            if ($buffer === false || $buffer === '') {
                break;
            }

            $position += strlen($buffer);

            if ($carry !== '') {
                $buffer = $carry . $buffer;
                $carry = '';
            }

            if ($position < $end) {
                $lastNewline = strrpos($buffer, "\n");

                if ($lastNewline === false) {
                    $carry = $buffer;
                    continue;
                }

                $carry = substr($buffer, $lastNewline + 1);
                $limit = $lastNewline + 1;
            } else {
                $limit = strlen($buffer);
            }

            $chunkProcessor($buffer, $limit, $counts, $firstSeen, $parsedDateCache, $remainingFirstSeen, $sequence, $uriCount);
        }

        if ($carry !== '') {
            $buffer = $carry . "\n";
            $chunkProcessor($buffer, strlen($buffer), $counts, $firstSeen, $parsedDateCache, $remainingFirstSeen, $sequence, $uriCount);
        }

        fclose($handle);

        return [$counts, $firstSeen];
    }


    private function resolveReadSize(): int
    {
        $override = getenv('PARSER_READ_SIZE');

        if ($override !== false && ctype_digit($override)) {
            return max(1_048_576, (int) $override);
        }

        return self::READ_SIZE;
    }

    private function buildOutput(array $counts, array $firstSeen): string
    {
        $uriCount = count(self::$uris);
        $orderedUris = [];

        foreach ($firstSeen as $uriIndex => $seen) {
            if ($seen !== -1) {
                $orderedUris[$uriIndex] = $seen;
            }
        }

        asort($orderedUris, SORT_NUMERIC);
        $buffer = "{\n";
        $uriKeys = array_keys($orderedUris);
        $uriTotal = count($uriKeys);

        foreach ($uriKeys as $uriOffset => $uriIndex) {
            $buffer .= '    ' . self::$encodedPaths[$uriIndex] . ": {\n";
            $hasVisit = false;

            for ($dayCode = 0, $slot = $uriIndex; $dayCode < self::DAY_RANGE; $dayCode++, $slot += $uriCount) {
                $count = $counts[$slot];

                if ($count !== 0) {
                    if ($hasVisit) {
                        $buffer .= ",\n";
                    }

                    $buffer .= '        "' . self::formatDayCode($dayCode) . '": ' . $count;
                    $hasVisit = true;
                }
            }

            $buffer .= "\n    }";

            if ($uriOffset !== $uriTotal - 1) {
                $buffer .= ",\n";
            } else {
                $buffer .= "\n";
            }
        }

        return $buffer . '}';
    }

    private static function formatDayCode(int $dayCode): string
    {
        return self::$dateCache[$dayCode] ??= sprintf(
            '%04d-%02d-%02d',
            2020 + ($dayCode >> 9),
            ($dayCode >> 5) & 0x0F,
            $dayCode & 0x1F,
        );
    }

    private function writeWorkerResult(string $path, array|string $counts, array $firstSeen): void
    {
        $handle = fopen($path, 'wb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open {$path}");
        }

        fwrite($handle, $this->encodeWorkerResult($counts, $firstSeen));

        fclose($handle);
    }

    private function writeWorkerStream($stream, array|string $counts, array $firstSeen): void
    {
        $payload = $this->encodeWorkerResult($counts, $firstSeen);
        $written = 0;
        $length = strlen($payload);

        while ($written < $length) {
            $chunk = fwrite($stream, substr($payload, $written));

            if ($chunk === false || $chunk === 0) {
                throw new RuntimeException('Unable to write worker payload');
            }

            $written += $chunk;
        }
    }

    private function encodeWorkerResult(array|string $counts, array $firstSeen): string
    {
        if ($this->shouldUseIgbinary()) {
            return igbinary_serialize([$firstSeen, $counts]);
        }

        $encodedSeen = [];

        foreach ($firstSeen as $index => $value) {
            $encodedSeen[$index] = $value === -1 ? 0xFFFFFFFF : $value;
        }

        return pack('V*', ...$encodedSeen) . $counts;
    }

    private function readWorkerResult(
        string $path,
        int $slotCount,
        int $uriCount,
        bool $rawCounts = false,
        bool $useByteCounts = false,
    ): array
    {
        if ($this->shouldUseIgbinary()) {
            $result = igbinary_unserialize(file_get_contents($path));

            if (! is_array($result) || count($result) !== 2) {
                throw new RuntimeException("Unable to read {$path}");
            }

            if (! $rawCounts && is_string($result[1])) {
                $result[1] = array_values(unpack('v*', $result[1]));
            }

            return $result;
        }

        $raw = file_get_contents($path);

        if ($raw === false) {
            throw new RuntimeException("Unable to read {$path}");
        }

        return $this->decodeWorkerResult($raw, $slotCount, $uriCount, $rawCounts, $useByteCounts);
    }

    private function decodeWorkerResult(
        string $raw,
        int $slotCount,
        int $uriCount,
        bool $rawCounts = false,
        bool $useByteCounts = false,
    ): array
    {
        if ($this->shouldUseIgbinary()) {
            $result = igbinary_unserialize($raw);

            if (! is_array($result) || count($result) !== 2) {
                throw new RuntimeException('Unable to decode worker payload');
            }

            if (! $rawCounts && is_string($result[1])) {
                $result[1] = array_values(unpack($useByteCounts ? 'C*' : 'v*', $result[1]));
            }

            return $result;
        }

        $headerSize = $uriCount * 4;
        $firstSeen = array_values(unpack('V*', substr($raw, 0, $headerSize)));
        $countSize = $useByteCounts ? $slotCount : $slotCount * 2;
        $counts = substr($raw, $headerSize, $countSize);

        if (! $rawCounts) {
            $counts = array_values(unpack($useByteCounts ? 'C*' : 'v*', $counts));
        }

        foreach ($firstSeen as $index => $value) {
            if ($value === 0xFFFFFFFF) {
                $firstSeen[$index] = -1;
            }
        }

        return [$counts, $firstSeen];
    }

    private function mergeWorkerResult(
        array|string|null &$counts,
        array &$firstSeen,
        array|string $workerCounts,
        array $workerSeen,
        int $workerIndex,
        int $slotCount,
        int $uriCount,
        bool $useSodiumMerge,
        bool $useByteCounts,
    ): void {
        if ($useSodiumMerge) {
            if ($counts === null) {
                $counts = $workerCounts;
            } else {
                sodium_add($counts, $workerCounts);
            }
        } else {
            if ($useByteCounts) {
                for ($slot = 0; $slot < $slotCount; $slot++) {
                    $counts[$slot] += ord($workerCounts[$slot]);
                }
            } else {
                for ($slot = 0; $slot < $slotCount; $slot++) {
                    $counts[$slot] += $workerCounts[$slot];
                }
            }
        }

        $workerPrefix = $workerIndex << 32;

        for ($uriIndex = 0; $uriIndex < $uriCount; $uriIndex++) {
            if ($workerSeen[$uriIndex] === -1) {
                continue;
            }

            $seen = $workerPrefix | $workerSeen[$uriIndex];

            if ($firstSeen[$uriIndex] === -1 || $seen < $firstSeen[$uriIndex]) {
                $firstSeen[$uriIndex] = $seen;
            }
        }
    }

    private function shouldUseIgbinary(): bool
    {
        return getenv('PARSER_USE_IGBINARY') !== false
            && function_exists('igbinary_serialize')
            && function_exists('igbinary_unserialize');
    }

    private function shouldUseDateCache(): bool
    {
        return getenv('PARSER_DISABLE_DATE_CACHE') === false;
    }

    private function shouldUseByteCounts(int $workerCount): bool
    {
        return $workerCount >= 4 && getenv('PARSER_DISABLE_BYTE_COUNTS') === false;
    }

    private function shouldUseSocketTransport(): bool
    {
        return function_exists('stream_socket_pair')
            && defined('STREAM_PF_UNIX')
            && getenv('PARSER_DISABLE_SOCKET_TRANSPORT') === false;
    }

    private function shouldProfile(): bool
    {
        return getenv('PARSER_PROFILE') !== false;
    }

}
