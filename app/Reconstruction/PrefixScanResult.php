<?php

namespace App\Reconstruction;

final class PrefixScanResult
{
    /**
     * @param list<int> $uriSequence
     * @param list<int> $timestampIds
     * @param list<string> $timestampValues
     * @param array<int, list<int>> $repeatPositions
     * @param list<int> $orderedUris
     * @param array<int, array{rows:int, unique_uris:int, unique_timestamps:int}> $milestones
     */
    public function __construct(
        public readonly int $requestedPrefixRows,
        public readonly int $verificationWindowRows,
        public readonly int $observedRows,
        public readonly array $uriSequence,
        public readonly array $timestampIds,
        public readonly array $timestampValues,
        public readonly array $repeatPositions,
        public readonly array $orderedUris,
        public readonly array $milestones,
    ) {}

    public function prefixRows(): int
    {
        return min($this->requestedPrefixRows, $this->observedRows);
    }
}
