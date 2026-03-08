<?php

namespace App\Reconstruction;

use Random\Engine\Xoshiro256StarStar;
use Random\Randomizer;

final class NativeRandomizer
{
    private function __construct(
        private readonly Xoshiro256StarStar $engine,
        private readonly Randomizer $randomizer,
    ) {}

    public static function fromSeed(int $seed): self
    {
        $engine = new Xoshiro256StarStar($seed);

        return new self($engine, new Randomizer($engine));
    }

    /**
     * @param array{0:array{},1:array{0:string,1:string,2:string,3:string}} $state
     */
    public static function fromState(array $state): self
    {
        $engine = new Xoshiro256StarStar(1);
        $engine->__unserialize($state);

        return new self($engine, new Randomizer($engine));
    }

    public function getInt(int $min, int $max): int
    {
        return $this->randomizer->getInt($min, $max);
    }

    /**
     * @return array{0:array{},1:array{0:string,1:string,2:string,3:string}}
     */
    public function exportState(): array
    {
        /** @var array{0:array{},1:array{0:string,1:string,2:string,3:string}} $state */
        $state = $this->engine->__serialize();

        return $state;
    }
}
