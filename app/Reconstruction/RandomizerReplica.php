<?php

namespace App\Reconstruction;

use GMP;

final class RandomizerReplica
{
    private const UINT32_RANGE = 4_294_967_296;

    private static ?GMP $uint64Range = null;

    public function __construct(private readonly Xoshiro256StarStarReplica $engine) {}

    public static function fromSeed(int $seed): self
    {
        return new self(Xoshiro256StarStarReplica::fromSeed($seed));
    }

    public function getInt(int $min, int $max): int
    {
        if ($max < $min) {
            throw new \ValueError('max must be greater than or equal to min');
        }

        $range = $max - $min;

        if ($range === 0) {
            return $min;
        }

        if ($range <= 0xFFFFFFFF) {
            $upper = $range + 1;
            $threshold = self::UINT32_RANGE - (self::UINT32_RANGE % $upper);

            do {
                $value = $this->engine->nextUInt32();
            } while ($value >= $threshold);

            return $min + ($value % $upper);
        }

        $upper = gmp_init((string) ($range + 1), 10);
        $threshold = gmp_sub(self::uint64Range(), gmp_mod(self::uint64Range(), $upper));

        do {
            $value = $this->engine->nextUInt64();
        } while (gmp_cmp($value, $threshold) >= 0);

        return $min + gmp_intval(gmp_mod($value, $upper));
    }

    public function copy(): self
    {
        return new self($this->engine->copy());
    }

    /** @return list<string> */
    public function exportState(): array
    {
        return $this->engine->exportState();
    }

    private static function uint64Range(): GMP
    {
        return self::$uint64Range ??= gmp_pow(2, 64);
    }
}
