<?php

namespace App\Reconstruction;

use Random\Engine\Xoshiro256StarStar;
use Random\Randomizer;
use RuntimeException;

final class ReconstructionAttempt
{
    private const DAY_RANGE = 4096;
    private const DATE_POOL_SIZE = 10_000;
    private const FIRST_SLUG_OFFSET = 25;
    private const FIVE_YEARS_IN_SECONDS = 157_680_000;
    private const ACTIVE_DAY_BITMAP_SIZE = self::DAY_RANGE >> 3;

    private static ?array $byteChars = null;
    private static ?array $bitMasks = null;

    /**
     * @param list<string> $uris
     */
    public function __construct(private readonly array $uris)
    {
        self::bootstrap();
    }

    /**
     * @param list<int> $milestones
     */
    public function scanPrefix(
        string $inputPath,
        int $requestedPrefixRows,
        int $verificationWindowRows,
        array $milestones = [100_000, 250_000, 500_000, 1_000_000],
    ): PrefixScanResult {
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new \RuntimeException("Unable to open {$inputPath}");
        }

        $limitRows = $requestedPrefixRows + $verificationWindowRows;
        $uriIndexByUri = array_flip($this->uris);
        $uriSequence = [];
        $timestampIds = [];
        $timestampIdByValue = [];
        $timestampValues = [];
        $repeatPositions = [];
        $orderedUris = [];
        $seenUris = [];
        $milestoneSnapshots = [];
        $nextMilestoneIndex = 0;
        sort($milestones, SORT_NUMERIC);
        $row = 0;

        while ($row < $limitRows && ($line = fgets($handle)) !== false) {
            $comma = strpos($line, ',', self::FIRST_SLUG_OFFSET);

            if ($comma === false) {
                continue;
            }

            $uri = substr($line, 0, $comma);
            $timestamp = substr($line, $comma + 1, 25);
            $uriIndex = $uriIndexByUri[$uri] ?? null;

            if ($uriIndex === null) {
                throw new \RuntimeException("Unknown URI {$uri}");
            }

            if (! isset($timestampIdByValue[$timestamp])) {
                $timestampIdByValue[$timestamp] = count($timestampValues);
                $timestampValues[] = $timestamp;
            }

            $timestampId = $timestampIdByValue[$timestamp];
            $uriSequence[] = $uriIndex;
            $timestampIds[] = $timestampId;
            $repeatPositions[$timestampId][] = $row;

            if (! isset($seenUris[$uriIndex])) {
                $seenUris[$uriIndex] = true;
                $orderedUris[] = $uriIndex;
            }

            $row++;

            while (
                $nextMilestoneIndex < count($milestones)
                && $row >= $milestones[$nextMilestoneIndex]
            ) {
                $milestone = $milestones[$nextMilestoneIndex];
                $milestoneSnapshots[$milestone] = [
                    'rows' => $row,
                    'unique_uris' => count($orderedUris),
                    'unique_timestamps' => count($timestampValues),
                ];
                $nextMilestoneIndex++;
            }
        }

        fclose($handle);

        return new PrefixScanResult(
            $requestedPrefixRows,
            $verificationWindowRows,
            $row,
            $uriSequence,
            $timestampIds,
            $timestampValues,
            $repeatPositions,
            $orderedUris,
            $milestoneSnapshots,
        );
    }

    /**
     * @param list<int> $milestones
     */
    public function attempt(
        string $inputPath,
        ?int $rows,
        ?int $seed,
        int $requestedPrefixRows,
        int $verificationWindowRows,
        array $milestones = [100_000, 250_000, 500_000, 1_000_000],
    ): ?ReconstructionResult {
        $scan = $this->scanPrefix($inputPath, $requestedPrefixRows, $verificationWindowRows, $milestones);

        return $this->attemptFromScan($inputPath, $scan, $rows, $seed);
    }

    public function attemptFromScan(
        string $inputPath,
        PrefixScanResult $scan,
        ?int $rows,
        ?int $seed,
    ): ?ReconstructionResult {
        if ($seed === null || $rows === null) {
            return null;
        }

        $precomputed = $this->attemptPrecomputedParallelReconstruction($inputPath, $scan, $rows, $seed);

        if ($precomputed instanceof ReconstructionResult) {
            return $precomputed;
        }

        [$randomizer, $datePoolStrings, $datePoolDayCodes] = $this->buildExecutionContext($seed);
        $uriCount = count($this->uris);
        $counts = str_repeat("\0", $uriCount * self::DAY_RANGE * 2);
        $firstSeen = array_fill(0, $uriCount, -1);
        $activeDays = self::emptyActiveDayBitmap();
        $prefixRows = $scan->prefixRows();
        $verificationRows = min($scan->verificationWindowRows, max(0, $scan->observedRows - $prefixRows));
        $midVerificationRows = min($scan->verificationWindowRows, max(0, $rows - intdiv($rows * 75, 100)));
        $midVerifier = $midVerificationRows > 0
            ? ApproximateWindowVerifier::fromFile(
                inputPath: $inputPath,
                uriIndexByUri: array_flip($this->uris),
                fileSize: filesize($inputPath) ?: 0,
                rows: $rows,
                targetRow: intdiv($rows * 75, 100),
                verificationRows: $midVerificationRows,
            )
            : null;

        if ($midVerificationRows > 0 && $midVerifier === null) {
            return null;
        }

        for ($row = 0; $row < $rows; $row++) {
            $uriIndex = $randomizer->getInt(0, $uriCount - 1);
            $timestampIndex = $randomizer->getInt(0, self::DATE_POOL_SIZE - 1);
            $timestamp = $datePoolStrings[$timestampIndex];

            if ($row < $scan->observedRows) {
                if ($uriIndex !== $scan->uriSequence[$row]) {
                    return null;
                }

                if ($timestamp !== $scan->timestampValues[$scan->timestampIds[$row]]) {
                    return null;
                }
            }

            if ($midVerifier !== null && ! $midVerifier->observe($row, $uriIndex, $timestamp)) {
                return null;
            }

            if ($firstSeen[$uriIndex] === -1) {
                $firstSeen[$uriIndex] = $row;
            }

            $dayCode = $datePoolDayCodes[$timestampIndex];
            self::markDayCode($activeDays, $dayCode);
            $slot = ($uriIndex << 13) + ($dayCode << 1);
            $lowByte = ord($counts[$slot]) + 1;

            if ($lowByte === 256) {
                $counts[$slot] = "\0";
                $slot++;
                $counts[$slot] = self::$byteChars[ord($counts[$slot]) + 1];
                continue;
            }

            $counts[$slot] = self::$byteChars[$lowByte];
        }

        if ($midVerifier !== null && ! $midVerifier->isVerified()) {
            return null;
        }

        return $this->finalizeCounts(
            $counts,
            $firstSeen,
            $activeDays,
            $rows,
            $seed,
            $prefixRows,
        );
    }

    private function attemptPrecomputedParallelReconstruction(
        string $inputPath,
        PrefixScanResult $scan,
        int $rows,
        int $seed,
    ): ?ReconstructionResult {
        $workers = $this->resolvePrecomputedWorkerCount();
        $precomputed = PrecomputedReconstructionStates::for($seed, $rows, $workers);

        if ($precomputed === null || ! function_exists('pcntl_fork')) {
            return null;
        }

        if (count($scan->orderedUris) !== count($this->uris)) {
            return null;
        }

        [, $datePoolStrings, $datePoolDayCodes] = $this->buildExecutionContext($seed);

        if (! $this->verifyPrefixWindow($scan, $datePoolStrings, $precomputed['states']['date_pool_done'])) {
            return null;
        }

        if (
            $this->shouldUseStrictMidVerification()
            && ! $this->verifyMidpointWindow($inputPath, $rows, $scan->verificationWindowRows, $datePoolStrings, $precomputed['states'][75_000_000] ?? null)
        ) {
            return null;
        }

        $counts = $this->buildCountsInParallelFromStates(
            $precomputed['states'],
            $precomputed['chunk_rows'],
            $workers,
            $rows,
            $datePoolDayCodes,
        );

        return $this->finalizeCounts(
            $counts,
            array_fill(0, count($this->uris), -1),
            $this->buildActiveDayBitmapFromDayCodes($datePoolDayCodes),
            $rows,
            $seed,
            $scan->prefixRows(),
            $scan->orderedUris,
        );
    }

    /**
     * @return array{0:Randomizer,1:list<string>,2:list<int>}
     */
    private function buildExecutionContext(int $seed): array
    {
        $randomizer = new Randomizer(new Xoshiro256StarStar($seed));
        $datePoolStrings = [];
        $datePoolDayCodes = [];

        for ($index = 0; $index < self::DATE_POOL_SIZE; $index++) {
            $timestamp = date('c', $seed - $randomizer->getInt(0, self::FIVE_YEARS_IN_SECONDS));
            $datePoolStrings[$index] = $timestamp;
            $datePoolDayCodes[$index] = self::dayCodeFromTimestamp($timestamp);
        }

        return [$randomizer, $datePoolStrings, $datePoolDayCodes];
    }

    private function finalizeCounts(
        string $counts,
        array $firstSeen,
        string $activeDays,
        int $rows,
        int $seed,
        int $prefixRows,
        ?array $orderedUris = null,
    ): ReconstructionResult
    {
        if ($orderedUris === null) {
            $ordered = [];

            foreach ($firstSeen as $uriIndex => $seen) {
                if ($seen !== -1) {
                    $ordered[$uriIndex] = $seen;
                }
            }

            asort($ordered, SORT_NUMERIC);
            $orderedUris = array_keys($ordered);
        }

        return new ReconstructionResult(
            $counts,
            $activeDays,
            $orderedUris,
            $rows,
            $seed,
            $prefixRows,
        );
    }

    /**
     * @param array{0:array{},1:array{0:string,1:string,2:string,3:string}} $state
     * @param list<string> $datePoolStrings
     */
    private function verifyPrefixWindow(PrefixScanResult $scan, array $datePoolStrings, array $state): bool
    {
        $engine = new Xoshiro256StarStar(1);
        $engine->__unserialize($state);
        $randomizer = new Randomizer($engine);
        $uriCount = count($this->uris);

        for ($row = 0; $row < $scan->observedRows; $row++) {
            $uriIndex = $randomizer->getInt(0, $uriCount - 1);
            $timestampIndex = $randomizer->getInt(0, self::DATE_POOL_SIZE - 1);

            if ($uriIndex !== $scan->uriSequence[$row]) {
                return false;
            }

            if ($datePoolStrings[$timestampIndex] !== $scan->timestampValues[$scan->timestampIds[$row]]) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param list<string> $datePoolStrings
     * @param array{0:array{},1:array{0:string,1:string,2:string,3:string}}|null $state
     */
    private function verifyMidpointWindow(
        string $inputPath,
        int $rows,
        int $verificationRows,
        array $datePoolStrings,
        ?array $state,
    ): bool {
        if ($verificationRows <= 0 || $state === null) {
            return true;
        }

        $verifier = ApproximateWindowVerifier::fromFile(
            inputPath: $inputPath,
            uriIndexByUri: array_flip($this->uris),
            fileSize: filesize($inputPath) ?: 0,
            rows: $rows,
            targetRow: 75_000_000,
            verificationRows: $verificationRows,
        );

        if ($verifier === null) {
            return false;
        }

        $engine = new Xoshiro256StarStar(1);
        $engine->__unserialize($state);
        $randomizer = new Randomizer($engine);
        $uriCount = count($this->uris);
        $startRow = 75_000_000;

        for ($offset = 0; $offset < $verificationRows; $offset++) {
            $uriIndex = $randomizer->getInt(0, $uriCount - 1);
            $timestamp = $datePoolStrings[$randomizer->getInt(0, self::DATE_POOL_SIZE - 1)];

            if (! $verifier->observe($startRow + $offset, $uriIndex, $timestamp)) {
                return false;
            }
        }

        return $verifier->isVerified();
    }

    /**
     * @param array<int|string, array{0:array{},1:array{0:string,1:string,2:string,3:string}}> $states
     * @param list<int> $datePoolDayCodes
     */
    private function buildCountsInParallelFromStates(
        array $states,
        int $chunkRows,
        int $workers,
        int $rows,
        array $datePoolDayCodes,
    ): string {
        $tempDir = sys_get_temp_dir() . '/reconstruct-counts-' . md5((string) microtime(true) . ':' . getmypid());

        if (! mkdir($tempDir, 0777, true) && ! is_dir($tempDir)) {
            throw new RuntimeException("Unable to create {$tempDir}");
        }

        $children = [];

        for ($workerIndex = 0; $workerIndex < $workers; $workerIndex++) {
            $startRow = $workerIndex * $chunkRows;
            $rowCount = min($chunkRows, $rows - $startRow);
            $resultPath = "{$tempDir}/worker-{$workerIndex}.bin";
            $state = $states[$startRow === 0 ? 'date_pool_done' : $startRow] ?? null;

            if ($state === null) {
                throw new RuntimeException("Missing precomputed state for row {$startRow}");
            }

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new RuntimeException('Unable to fork reconstruction worker');
            }

            if ($pid === 0) {
                file_put_contents($resultPath, $this->buildCountsFromState($state, $rowCount, $datePoolDayCodes));
                exit(0);
            }

            $children[$pid] = $resultPath;
        }

        $counts = null;
        $status = 0;

        while ($children !== []) {
            $pid = pcntl_wait($status);

            if ($pid <= 0) {
                break;
            }

            $path = $children[$pid] ?? null;

            if ($path === null) {
                continue;
            }

            if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                throw new RuntimeException("Reconstruction worker {$pid} failed");
            }

            $workerCounts = file_get_contents($path);

            if ($workerCounts === false) {
                throw new RuntimeException("Unable to read {$path}");
            }

            if ($counts === null) {
                $counts = $workerCounts;
            } elseif (function_exists('sodium_add')) {
                sodium_add($counts, $workerCounts);
            } else {
                $this->mergeCountStrings($counts, $workerCounts);
            }

            @unlink($path);
            unset($children[$pid]);
        }

        @rmdir($tempDir);

        return $counts ?? str_repeat("\0", count($this->uris) * self::DAY_RANGE * 2);
    }

    /**
     * @param array{0:array{},1:array{0:string,1:string,2:string,3:string}} $state
     * @param list<int> $datePoolDayCodes
     */
    private function buildCountsFromState(array $state, int $rowCount, array $datePoolDayCodes): string
    {
        $engine = new Xoshiro256StarStar(1);
        $engine->__unserialize($state);
        $randomizer = new Randomizer($engine);
        $uriCount = count($this->uris);
        $counts = str_repeat("\0", $uriCount * self::DAY_RANGE * 2);

        for ($row = 0; $row < $rowCount; $row++) {
            $uriIndex = $randomizer->getInt(0, $uriCount - 1);
            $dayCode = $datePoolDayCodes[$randomizer->getInt(0, self::DATE_POOL_SIZE - 1)];
            $slot = ($uriIndex << 13) + ($dayCode << 1);
            $lowByte = ord($counts[$slot]) + 1;

            if ($lowByte === 256) {
                $counts[$slot] = "\0";
                $slot++;
                $counts[$slot] = self::$byteChars[ord($counts[$slot]) + 1];
                continue;
            }

            $counts[$slot] = self::$byteChars[$lowByte];
        }

        return $counts;
    }

    private function mergeCountStrings(string &$counts, string $workerCounts): void
    {
        $length = strlen($counts);
        $carry = 0;

        for ($index = 0; $index < $length; $index++) {
            $sum = ord($counts[$index]) + ord($workerCounts[$index]) + $carry;
            $counts[$index] = self::$byteChars[$sum & 0xFF];
            $carry = $sum >> 8;

            if (($index & 1) === 1) {
                $carry = 0;
            }
        }
    }

    /**
     * @param list<int> $dayCodes
     */
    private function buildActiveDayBitmapFromDayCodes(array $dayCodes): string
    {
        $bitmap = self::emptyActiveDayBitmap();

        foreach ($dayCodes as $dayCode) {
            self::markDayCode($bitmap, $dayCode);
        }

        return $bitmap;
    }

    private function resolvePrecomputedWorkerCount(): int
    {
        $override = getenv('PARSER_RECONSTRUCT_WORKERS');

        if ($override !== false && ctype_digit($override)) {
            return max(1, (int) $override);
        }

        return 10;
    }

    private function shouldUseStrictMidVerification(): bool
    {
        $value = getenv('PARSER_ENABLE_RECONSTRUCT_MID_VERIFY');

        if ($value === false) {
            return false;
        }

        return in_array(strtolower($value), ['1', 'true', 'yes', 'on'], true);
    }

    private static function dayCodeFromTimestamp(string $timestamp): int
    {
        return (((int) substr($timestamp, 2, 2) - 20) << 9)
            | (((int) substr($timestamp, 5, 2)) << 5)
            | (int) substr($timestamp, 8, 2);
    }

    private static function emptyActiveDayBitmap(): string
    {
        return str_repeat("\0", self::ACTIVE_DAY_BITMAP_SIZE);
    }

    private static function markDayCode(string &$bitmap, int $dayCode): void
    {
        $index = $dayCode >> 3;
        $bitmap[$index] = self::$byteChars[ord($bitmap[$index]) | self::$bitMasks[$dayCode & 7]];
    }

    private static function bootstrap(): void
    {
        if (self::$byteChars !== null) {
            return;
        }

        self::$byteChars = [];

        for ($byte = 0; $byte < 256; $byte++) {
            self::$byteChars[$byte] = chr($byte);
        }

        self::$bitMasks = [
            1,
            2,
            4,
            8,
            16,
            32,
            64,
            128,
        ];
    }
}
