<?php

namespace App\Reconstruction;

use Random\Engine\Xoshiro256StarStar;
use Random\Randomizer;

final class FeasibilityHarness
{
    private const DATE_POOL_SIZE = 10_000;
    private const FIVE_YEARS_IN_SECONDS = 157_680_000;

    /**
     * @param list<string> $uris
     */
    public function __construct(private readonly array $uris) {}

    /**
     * @return array{
     *   rng_mismatches:list<string>,
     *   datasets:list<array{
     *     path:string,
     *     observed_rows:int,
     *     unique_uris:int,
     *     unique_timestamps:int,
     *     reconstructable:bool
     *   }>
     * }
     */
    public function run(
        string $outputDirectory,
        int $rows,
        int $datasets,
        int $requestedPrefixRows,
        int $verificationWindowRows,
    ): array {
        if (! is_dir($outputDirectory) && ! mkdir($outputDirectory, 0777, true) && ! is_dir($outputDirectory)) {
            throw new \RuntimeException("Unable to create {$outputDirectory}");
        }

        $attempt = new ReconstructionAttempt($this->uris);
        $report = [
            'rng_mismatches' => $attempt->validateReplica(
                seeds: [1, 1772177204, 20260308],
                ranges: [[0, 267], [0, 9_999], [0, self::FIVE_YEARS_IN_SECONDS]],
            ),
            'datasets' => [],
        ];

        for ($dataset = 0; $dataset < $datasets; $dataset++) {
            $path = rtrim($outputDirectory, '/') . "/dataset-{$dataset}.csv";
            $this->generateUnseededDataset($path, $rows);
            $scan = $attempt->scanPrefix($path, $requestedPrefixRows, $verificationWindowRows);
            $result = $attempt->attemptFromScan($path, $scan, $rows, null);

            $report['datasets'][] = [
                'path' => $path,
                'observed_rows' => $scan->observedRows,
                'unique_uris' => count($scan->orderedUris),
                'unique_timestamps' => count($scan->timestampValues),
                'reconstructable' => $result !== null,
            ];
        }

        return $report;
    }

    private function generateUnseededDataset(string $outputPath, int $rows): void
    {
        $randomizer = new Randomizer(new Xoshiro256StarStar());
        $now = time();
        $datePool = [];

        for ($index = 0; $index < self::DATE_POOL_SIZE; $index++) {
            $datePool[$index] = date('c', $now - $randomizer->getInt(0, self::FIVE_YEARS_IN_SECONDS));
        }

        $handle = fopen($outputPath, 'wb');

        if ($handle === false) {
            throw new \RuntimeException("Unable to open {$outputPath}");
        }

        $buffer = '';
        $uriCount = count($this->uris);

        for ($row = 0; $row < $rows; $row++) {
            $buffer .= $this->uris[$randomizer->getInt(0, $uriCount - 1)]
                . ','
                . $datePool[$randomizer->getInt(0, self::DATE_POOL_SIZE - 1)]
                . "\n";

            if (($row + 1) % 10_000 === 0) {
                fwrite($handle, $buffer);
                $buffer = '';
            }
        }

        if ($buffer !== '') {
            fwrite($handle, $buffer);
        }

        fclose($handle);
    }
}
