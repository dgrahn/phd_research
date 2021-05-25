# phd_research

```
This is for past me, not current me or you. There's no guarantee that any of
this will ever work except the one time that I ran it. ;)
```

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