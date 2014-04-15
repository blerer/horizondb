grammar Hql;

fragment A_ :   'a' | 'A';
fragment B_ :   'b' | 'B';
fragment C_ :   'c' | 'C';
fragment D_ :   'd' | 'D';
fragment E_ :   'e' | 'E';
fragment F_ :   'f' | 'F';
fragment G_ :   'g' | 'G';
fragment H_ :   'h' | 'H';
fragment I_ :   'i' | 'I';
fragment J_ :   'j' | 'J';
fragment K_ :   'k' | 'K';
fragment L_ :   'l' | 'L';
fragment M_ :   'm' | 'M';
fragment N_ :   'n' | 'N';
fragment O_ :   'o' | 'O';
fragment P_ :   'p' | 'P';
fragment Q_ :   'q' | 'Q';
fragment R_ :   'r' | 'R';
fragment S_ :   's' | 'S';
fragment T_ :   't' | 'T';
fragment U_ :   'u' | 'U';
fragment V_ :   'v' | 'V';
fragment W_ :   'w' | 'W';
fragment X_ :   'x' | 'X';
fragment Y_ :   'y' | 'Y';
fragment Z_ :   'z' | 'Z';

BYTE: B_ Y_ T_ E_ ;
CREATE: C_ R_ E_ A_ T_ E_ ;
DATABASE: D_ A_ T_ A_ B_ A_ S_ E_ ;
DECIMAL: D_ E_ C_ I_ M_ A_ L_ ;
IN: I_ N_ ;
INTEGER: I_ N_ T_ E_ G_ E_ R_ ;
LONG: L_ O_ N_ G_ ;
MICROSECONDS: M_ I_ C_ R_ O_ S_ E_ C_ O_ N_ D_ S_ ;
MILLISECONDS: M_ I_ L_ L_ I_ S_ E_ C_ O_ N_ D_ S_ ;
NANOSECONDS: N_ A_ N_ O_ S_ E_ C_ O_ N_ D_ S_ ;
NANOSECONDS_TIMESTAMP: NANOSECONDS'_'TIMESTAMP;
SECONDS: S_ E_ C_ O_ N_ D_ S_ ;
TIMESTAMP: T_ I_ M_ E_ S_ T_ A_ M_ P_ ;
TIMESERIES: T_ I_ M_ E_ S_ E_ R_ I_ E_ S_;
USE: U_ S_ E_ ;

statements: (statement ';')* ;
statement: createDatabase |
           useDatabase |
           createTimeseries ;
createDatabase: CREATE DATABASE ID ;
createTimeseries: CREATE TIMESERIES ID '(' recordsDefinition ')' ;
recordsDefinition: recordDefinition (',' recordDefinition)* ;
recordDefinition: ID '(' fieldsDefinition ')' ;
fieldsDefinition: fieldDefinition (',' fieldDefinition)* ;
fieldDefinition:  ID type;
type: BYTE |
      INTEGER |
      LONG |
      DECIMAL |
      NANOSECONDS_TIMESTAMP |
      MICROSECONDS'_'TIMESTAMP |
      MILLISECONDS'_'TIMESTAMP |
      SECONDS'_'TIMESTAMP; 
useDatabase: USE ID;
ID: ID_LETTER (ID_LETTER | DIGIT)* ;
fragment ID_LETTER: 'a'..'z'|'A'..'Z'|'_' ;
fragment DIGIT : '0'..'9' ;
WS: [ \t\r\n]+ -> skip ;