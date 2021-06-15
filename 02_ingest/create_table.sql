CREATE TABLE tokens (
    uuid VARCHAR(36) PRIMARY KEY,
    dataset VARCHAR(4) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    token_num INTEGER NOT NULL,
    char_start INTEGER NOT NULL,
    char_end INTEGER NOT NULL,
    token_text TEXT NOT NULL,
    token_type VARCHAR(255) NOT NULL,
    channel INTEGER,
    line INTEGER NOT NULL,
    line_char INTEGER NOT NULL);