# phd_research

1. Download and prepare datasets.
2. Parse using antlr
3. Convert to CSVs
4. Analyze

## Setup Antlr4
```
java org.antlr.v4.Tool CPP14Lexer.g4
java org.antlr.v4.Tool CPP14Parser.g4
javac *.java
```