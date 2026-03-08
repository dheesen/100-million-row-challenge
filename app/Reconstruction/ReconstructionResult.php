<?php

namespace App\Reconstruction;

final class ReconstructionResult
{
    /**
     * @param list<int> $orderedUris
     */
    public function __construct(
        public readonly string $counts,
        public readonly string $activeDayBitmap,
        public readonly array $orderedUris,
        public readonly int $rows,
        public readonly int $seed,
        public readonly int $prefixRows,
    ) {}
}
