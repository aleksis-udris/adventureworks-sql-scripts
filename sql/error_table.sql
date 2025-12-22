DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.error_records;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.error_records (
    ErrorID UInt64,
    ErrorDate DateTime,
    SourceTable String,
    RecordNaturalKey String,
    ErrorType String,
    ErrorSeverity String,
    ErrorMessage String,
    FailedData String,
    ProcessingBatchID String,
    TaskName String,
    IsRecoverable UInt8,
    RetryCount UInt8 DEFAULT 0,
    LastAttemptDate Nullable(DateTime),
    IsResolved UInt8 DEFAULT 0,
    ResolutionComment Nullable(String)
) ENGINE = MergeTree()
ORDER BY (ErrorDate, SourceTable, ErrorType)
PARTITION BY toYYYYMM(ErrorDate);