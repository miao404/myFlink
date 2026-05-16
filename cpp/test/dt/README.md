# OmniStream DT Fuzz Testing Framework

DT (Design for Testability) fuzz testing framework for OmniStream C++ operators. Uses the DTFrame infrastructure to perform automated fuzz testing with varied inputs covering data types, scales, distributions, RowKind combinations, and boundary conditions.

## Directory Structure

```
dt/
├── README.md
├── CMakeLists.txt
├── common/                          # Shared utilities
│   ├── CMakeLists.txt
│   ├── dt_fuzz_data.h               # Fuzz data structures and enums
│   ├── dt_fuzz_factory_util.h/cpp   # Operator factory creation utilities
│   └── runtime_env_util.h/cpp       # Runtime environment initialization
├── table_operators/                  # Table (SQL) operator fuzz tests
│   ├── sourcetree/
│   │   ├── table_fuzz_wrapper.h/cpp # Entry point and dispatcher
│   │   ├── aggregate_fuzz.cpp       # GroupAggFunction (SUM/COUNT/AVG/MAX/MIN)
│   │   ├── deduplicate_fuzz.cpp     # RowTimeDeduplicateFunction
│   │   ├── join_fuzz.cpp            # StreamingJoinOperator (Inner/LeftOuter)
│   │   └── rank_fuzz.cpp            # AppendOnlyTopNFunction/FastTop1Function
│   └── testtree/
│       ├── dtframe.cfg              # DTFrame configuration
│       └── cases/*.json             # Test case definitions
└── streaming_operators/              # DataStream operator fuzz tests
    ├── sourcetree/
    │   ├── streaming_fuzz_wrapper.h/cpp  # Entry point and dispatcher
    │   ├── keyed_process_fuzz.cpp   # KeyedProcessOperator
    │   ├── co_process_fuzz.cpp      # KeyedCoProcessOperator
    │   └── transform_fuzz.cpp       # StreamFilter/StreamMap/StreamFlatMap
    └── testtree/
        ├── dtframe.cfg              # DTFrame configuration
        └── cases/*.json             # Test case definitions
```

## Operator Coverage

### Table Operators

| Operator | Class | Test Modes |
|----------|-------|------------|
| Aggregate | GroupAggFunction | SUM, COUNT, AVG, MAX, MIN |
| Deduplicate | RowTimeDeduplicateFunction | keepLastRow, keepFirstRow |
| Join | StreamingJoinOperator | InnerJoin, LeftOuterJoin |
| Rank | AppendOnlyTopNFunction, FastTop1Function | TopN, Top1 |

### Streaming Operators

| Operator | Class | Test Modes |
|----------|-------|------------|
| KeyedProcess | KeyedProcessOperator | GroupAgg, MockUDF, MultiKey |
| CoProcess | KeyedCoProcessOperator | ConfigParsing, DualInput |
| Transform | StreamFilter/Map/FlatMap | Filter, Map, FlatMap |

## Supported Data Types

- `BIGINT` (INT64)
- `INTEGER` (INT32)
- `VARCHAR` / `STRING`
- `BOOLEAN`
- `TIMESTAMP_WITHOUT_TIME_ZONE` (precision 0-3)
- `DECIMAL64` / `DECIMAL128`

## State Backends

- HashMapStateBackend (default, recommended for fuzz testing)
- RocksDB (available, use with caution for serialization)

## Build

Requires DTFrame environment (`$DT_FRAME` set):

```bash
cd myFlink/cpp
mkdir -p build && cd build
cmake .. -DBUILD_DT=ON
make -j$(nproc)
```

## Run Tests

```bash
# Table operators
cd test/dt/table_operators/testtree
$DT_FRAME/dist/bin/dt_engine --config dtframe.cfg

# Streaming operators
cd test/dt/streaming_operators/testtree
$DT_FRAME/dist/bin/dt_engine --config dtframe.cfg
```

## Test Case Configuration

JSON files in `cases/` directories define test metadata:
- `suite` / `case`: Test identification
- `fuzz_function`: Entry point function name
- `parameters`: Fuzz input parameter definitions
- `test_points`: Covered test scenarios with coverage tags
- `data_distributions`: Data distribution patterns tested
- `boundary_conditions`: Edge cases covered

## Adding New Tests

1. Add fuzz implementation in the appropriate `sourcetree/` directory
2. Declare the function in the corresponding `*_fuzz_wrapper.h`
3. Add dispatch case in `*_fuzz_wrapper.cpp`
4. Create a JSON test case file in `testtree/cases/`
5. Rebuild and run
