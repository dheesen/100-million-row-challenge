<?php

namespace App\Reconstruction;

final class ApproximateWindowVerifier
{
    private const FIRST_SLUG_OFFSET = 25;
    private const DEFAULT_ANCHOR_ROWS = 16;
    private const DEFAULT_SEARCH_RADIUS_ROWS = 2_097_152;
    private const MIN_SAMPLE_BYTES = 16_777_216;
    private const SAMPLE_WINDOW_MULTIPLIER = 128;

    /** @var list<string> */
    private array $rollingTokens = [];

    private ?int $matchedRowStart = null;
    private ?int $matchedOffset = null;
    private bool $verified = false;

    /**
     * @param list<string> $sampleTokens
     */
    private function __construct(
        private readonly int $approxStartRow,
        private readonly int $verificationRows,
        private readonly int $searchRadiusRows,
        private readonly int $anchorRows,
        private readonly array $sampleTokens,
        private readonly string $anchorKey,
    ) {}

    /**
     * @param array<string, int> $uriIndexByUri
     */
    public static function fromFile(
        string $inputPath,
        array $uriIndexByUri,
        int $fileSize,
        int $rows,
        int $targetRow,
        int $verificationRows,
    ): ?self {
        if ($rows <= 0 || $verificationRows <= 0 || $fileSize <= 0) {
            return null;
        }

        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            return null;
        }

        $averageLineLength = max(1, intdiv($fileSize, $rows));
        $desiredByteOffset = intdiv($targetRow * $fileSize, $rows);
        $sampleBytes = max(
            self::MIN_SAMPLE_BYTES,
            $verificationRows * $averageLineLength * self::SAMPLE_WINDOW_MULTIPLIER,
        );
        $startByteOffset = max(0, $desiredByteOffset - intdiv($sampleBytes, 2));

        if (fseek($handle, $startByteOffset) !== 0) {
            fclose($handle);

            return null;
        }

        if ($startByteOffset !== 0) {
            fgets($handle);
        }

        $alignedByteOffset = ftell($handle);

        if (! is_int($alignedByteOffset)) {
            fclose($handle);

            return null;
        }

        $sampleTokens = [];

        while (count($sampleTokens) < $verificationRows && ($line = fgets($handle)) !== false) {
            $comma = strpos($line, ',', self::FIRST_SLUG_OFFSET);

            if ($comma === false) {
                continue;
            }

            $uri = substr($line, 0, $comma);
            $uriIndex = $uriIndexByUri[$uri] ?? null;

            if ($uriIndex === null) {
                fclose($handle);

                return null;
            }

            $sampleTokens[] = self::token($uriIndex, substr($line, $comma + 1, 25));
        }

        fclose($handle);

        $anchorRows = min(self::DEFAULT_ANCHOR_ROWS, count($sampleTokens));

        if ($anchorRows === 0 || count($sampleTokens) < $verificationRows) {
            return null;
        }

        return new self(
            approxStartRow: intdiv($alignedByteOffset, $averageLineLength),
            verificationRows: $verificationRows,
            searchRadiusRows: max(self::DEFAULT_SEARCH_RADIUS_ROWS, $verificationRows << 4),
            anchorRows: $anchorRows,
            sampleTokens: $sampleTokens,
            anchorKey: self::buildAnchorKey(array_slice($sampleTokens, 0, $anchorRows)),
        );
    }

    public function observe(int $row, int $uriIndex, string $timestamp): bool
    {
        if ($this->verified) {
            return true;
        }

        $token = self::token($uriIndex, $timestamp);

        if ($this->matchedOffset !== null) {
            $expectedRow = $this->matchedRowStart + $this->matchedOffset;

            if ($row < $expectedRow) {
                return true;
            }

            if ($row !== $expectedRow) {
                return false;
            }

            if ($token !== $this->sampleTokens[$this->matchedOffset]) {
                return false;
            }

            $this->matchedOffset++;

            if ($this->matchedOffset >= $this->verificationRows) {
                $this->verified = true;
            }

            return true;
        }

        $searchStart = max(0, $this->approxStartRow - $this->searchRadiusRows);
        $searchEnd = $this->approxStartRow + $this->verificationRows + $this->searchRadiusRows;

        if ($row < $searchStart || $row > $searchEnd) {
            return true;
        }

        $this->rollingTokens[] = $token;

        if (count($this->rollingTokens) > $this->anchorRows) {
            array_shift($this->rollingTokens);
        }

        if (count($this->rollingTokens) !== $this->anchorRows) {
            return true;
        }

        if (self::buildAnchorKey($this->rollingTokens) !== $this->anchorKey) {
            return true;
        }

        $this->matchedRowStart = $row - $this->anchorRows + 1;
        $this->matchedOffset = $this->anchorRows;

        if ($this->matchedOffset >= $this->verificationRows) {
            $this->verified = true;
        }

        return true;
    }

    public function isVerified(): bool
    {
        return $this->verified;
    }

    private static function token(int $uriIndex, string $timestamp): string
    {
        return pack('n', $uriIndex) . $timestamp;
    }

    /**
     * @param list<string> $tokens
     */
    private static function buildAnchorKey(array $tokens): string
    {
        return implode("\n", $tokens);
    }
}
