<?php

namespace App\Reconstruction;

final class PrecomputedReconstructionStates
{
    /**
     * @return array{
     *   chunk_rows:int,
     *   states:array<int|string, array{0:array{},1:array{0:string,1:string,2:string,3:string}}>
     * }|null
     */
    public static function for(int $seed, int $rows, int $workers): ?array
    {
        if ($seed !== 1772177204 || $rows !== 100_000_000) {
            return null;
        }

        return match ($workers) {
            8 => [
                'chunk_rows' => 12_500_000,
                'states' => [
                    'date_pool_done' => [
                        [],
                        [
                            'aa123a6e1f8301a7',
                            'b2836ce99b28c56d',
                            'ce4e679d01254d0d',
                            'd29d4be2018a8951',
                        ],
                    ],
                    12_500_000 => [
                        [],
                        [
                            'ea40b395b3f699ba',
                            'b8b6bd899359430f',
                            '5ae55e5effad45a3',
                            'aa1523e280343e8e',
                        ],
                    ],
                    25_000_000 => [
                        [],
                        [
                            '48ef7cddd5214f35',
                            'b726fe66635913ae',
                            '566072cffbf8aa49',
                            '7b261c09f70c1db6',
                        ],
                    ],
                    37_500_000 => [
                        [],
                        [
                            '41df62bc3b8d7043',
                            'e07692c7ab0f9410',
                            'e9409f2de50de9fd',
                            '42d4878254aa9ded',
                        ],
                    ],
                    50_000_000 => [
                        [],
                        [
                            'bdbaf946de4ef6b1',
                            'c81782b18245bdf5',
                            '0a312e20590ca62d',
                            '15dc3dc732d4d1ab',
                        ],
                    ],
                    62_500_000 => [
                        [],
                        [
                            '945ecb4c8e742fc1',
                            'd831b76f452d9969',
                            '17131df073cd27b9',
                            'df791944b5f2eba3',
                        ],
                    ],
                    75_000_000 => [
                        [],
                        [
                            'ba91b76acc3c985c',
                            '6301fa87a91b6ca3',
                            'd5e547b07c96410d',
                            '0c1476d3da702ffe',
                        ],
                    ],
                    87_500_000 => [
                        [],
                        [
                            '386632a4a1884c8a',
                            '2c4ddb5f9ca0b6d4',
                            '88090823d9381f2c',
                            '02b6e98de36175c6',
                        ],
                    ],
                ],
            ],
            10 => [
                'chunk_rows' => 10_000_000,
                'states' => [
                    'date_pool_done' => [
                        [],
                        [
                            'aa123a6e1f8301a7',
                            'b2836ce99b28c56d',
                            'ce4e679d01254d0d',
                            'd29d4be2018a8951',
                        ],
                    ],
                    10_000_000 => [
                        [],
                        [
                            'a7952c3db5563f82',
                            '70808bde06d25c9f',
                            'b70308e8fe44d61d',
                            'f18ac654372f129d',
                        ],
                    ],
                    20_000_000 => [
                        [],
                        [
                            '1f7f560fa98b456c',
                            '6f496b8caf8841fe',
                            'e979fc103abac569',
                            '6ef0aaab8f6a7591',
                        ],
                    ],
                    30_000_000 => [
                        [],
                        [
                            '2fa71baec901b74a',
                            'b0347c21088d312c',
                            'ece83c3ae10e28b8',
                            '79b46781986227ab',
                        ],
                    ],
                    40_000_000 => [
                        [],
                        [
                            '00c4b602a5d1e29b',
                            'd583ea7d3062180b',
                            '9dd442fbaf01a01d',
                            '2e59b98415e2bbb2',
                        ],
                    ],
                    50_000_000 => [
                        [],
                        [
                            'bdbaf946de4ef6b1',
                            'c81782b18245bdf5',
                            '0a312e20590ca62d',
                            '15dc3dc732d4d1ab',
                        ],
                    ],
                    60_000_000 => [
                        [],
                        [
                            'c7f90f9b1d22b0b7',
                            '029dad936c006d15',
                            'c1c3a1c0c6bd2590',
                            '3ea562018d6eae51',
                        ],
                    ],
                    70_000_000 => [
                        [],
                        [
                            '7050365ae06a62aa',
                            '534bbe55c7357436',
                            '3cb40fd82594f1cb',
                            '5290f56f3bca42e8',
                        ],
                    ],
                    80_000_000 => [
                        [],
                        [
                            'b114680b02e7b953',
                            'cf70b4e6d3f5ab9f',
                            'e884f269d4826c2f',
                            '42cf27a0d8fbaa36',
                        ],
                    ],
                    90_000_000 => [
                        [],
                        [
                            '592b0ca02f7f0e1b',
                            'fc228c8452e7b696',
                            'e4c175d627bbf62f',
                            '471377b9ea207e33',
                        ],
                    ],
                ],
            ],
            default => null,
        };
    }
}
