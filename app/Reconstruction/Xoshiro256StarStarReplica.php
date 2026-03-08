<?php

namespace App\Reconstruction;

use GMP;
use RuntimeException;

final class Xoshiro256StarStarReplica
{
    private const MASK_64_HEX = 'FFFFFFFFFFFFFFFF';
    private const MASK_32_HEX = 'FFFFFFFF';
    private const SPLITMIX_INCREMENT_HEX = '9E3779B97F4A7C15';
    private const SPLITMIX_MUL_1_HEX = 'BF58476D1CE4E5B9';
    private const SPLITMIX_MUL_2_HEX = '94D049BB133111EB';

    /** @var array<int, GMP> */
    private array $state;

    private static ?GMP $mask64 = null;
    private static ?GMP $mask32 = null;
    private static ?GMP $splitmixIncrement = null;
    private static ?GMP $splitmixMul1 = null;
    private static ?GMP $splitmixMul2 = null;
    private static ?GMP $pow2_64 = null;

    private function __construct(GMP $s0, GMP $s1, GMP $s2, GMP $s3)
    {
        self::assertSupported();

        $this->state = [$s0, $s1, $s2, $s3];
    }

    public static function fromSeed(int $seed): self
    {
        self::assertSupported();

        $splitMixState = self::unsignedFromInt($seed);

        return new self(
            self::splitMix64($splitMixState),
            self::splitMix64($splitMixState),
            self::splitMix64($splitMixState),
            self::splitMix64($splitMixState),
        );
    }

    public function nextUInt32(): int
    {
        return gmp_intval(gmp_and($this->nextUInt64(), self::mask32()));
    }

    public function nextUInt64(): GMP
    {
        $result = self::mul64(self::rotl(self::mul64($this->state[1], 5), 7), 9);
        $t = self::shl64($this->state[1], 17);

        $this->state[2] = gmp_xor($this->state[2], $this->state[0]);
        $this->state[3] = gmp_xor($this->state[3], $this->state[1]);
        $this->state[1] = gmp_xor($this->state[1], $this->state[2]);
        $this->state[0] = gmp_xor($this->state[0], $this->state[3]);
        $this->state[2] = gmp_xor($this->state[2], $t);
        $this->state[3] = self::rotl($this->state[3], 45);

        return gmp_and($result, self::mask64());
    }

    /** @return list<string> */
    public function exportState(): array
    {
        return [
            gmp_strval($this->state[0]),
            gmp_strval($this->state[1]),
            gmp_strval($this->state[2]),
            gmp_strval($this->state[3]),
        ];
    }

    public function copy(): self
    {
        return new self(
            gmp_init(gmp_strval($this->state[0]), 10),
            gmp_init(gmp_strval($this->state[1]), 10),
            gmp_init(gmp_strval($this->state[2]), 10),
            gmp_init(gmp_strval($this->state[3]), 10),
        );
    }

    private static function splitMix64(GMP &$state): GMP
    {
        $state = self::add64($state, self::splitmixIncrement());
        $z = $state;
        $z = self::mul64(gmp_xor($z, self::shr64($z, 30)), self::splitmixMul1());
        $z = self::mul64(gmp_xor($z, self::shr64($z, 27)), self::splitmixMul2());

        return gmp_and(gmp_xor($z, self::shr64($z, 31)), self::mask64());
    }

    private static function rotl(GMP $value, int $shift): GMP
    {
        $shift %= 64;

        if ($shift === 0) {
            return gmp_and($value, self::mask64());
        }

        return gmp_and(
            gmp_or(
                self::shl64($value, $shift),
                self::shr64($value, 64 - $shift),
            ),
            self::mask64(),
        );
    }

    private static function add64(GMP $left, GMP $right): GMP
    {
        return gmp_and(gmp_add($left, $right), self::mask64());
    }

    private static function mul64(GMP $left, int|GMP $right): GMP
    {
        return gmp_and(
            gmp_mul($left, $right instanceof GMP ? $right : gmp_init((string) $right, 10)),
            self::mask64(),
        );
    }

    private static function shl64(GMP $value, int $shift): GMP
    {
        return gmp_and(gmp_mul($value, gmp_pow(2, $shift)), self::mask64());
    }

    private static function shr64(GMP $value, int $shift): GMP
    {
        if ($shift === 0) {
            return gmp_and($value, self::mask64());
        }

        return gmp_div_q($value, gmp_pow(2, $shift));
    }

    private static function unsignedFromInt(int $value): GMP
    {
        if ($value >= 0) {
            return gmp_init((string) $value, 10);
        }

        return gmp_add(gmp_init((string) $value, 10), self::pow2_64());
    }

    private static function assertSupported(): void
    {
        if (! function_exists('gmp_init')) {
            throw new RuntimeException('GMP extension is required for reconstruction');
        }
    }

    private static function mask64(): GMP
    {
        return self::$mask64 ??= gmp_init(self::MASK_64_HEX, 16);
    }

    private static function mask32(): GMP
    {
        return self::$mask32 ??= gmp_init(self::MASK_32_HEX, 16);
    }

    private static function splitmixIncrement(): GMP
    {
        return self::$splitmixIncrement ??= gmp_init(self::SPLITMIX_INCREMENT_HEX, 16);
    }

    private static function splitmixMul1(): GMP
    {
        return self::$splitmixMul1 ??= gmp_init(self::SPLITMIX_MUL_1_HEX, 16);
    }

    private static function splitmixMul2(): GMP
    {
        return self::$splitmixMul2 ??= gmp_init(self::SPLITMIX_MUL_2_HEX, 16);
    }

    private static function pow2_64(): GMP
    {
        return self::$pow2_64 ??= gmp_pow(2, 64);
    }
}
