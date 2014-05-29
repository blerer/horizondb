grammar Hql;

fragment A_ 
    : 'a' 
    | 'A'
    ;
    
fragment B_ 
    : 'b'
    | 'B'
    ;
    
fragment C_ 
    : 'c' 
    | 'C'
    ;
    
fragment D_ 
    : 'd' 
    | 'D'
    ;
    
fragment E_ 
    : 'e' 
    | 'E'
    ;
    
fragment F_ 
    : 'f' 
    | 'F'
    ;
    
fragment G_ 
    : 'g' 
    | 'G'
    ;
    
fragment H_ 
    : 'h' 
    | 'H'
    ;
    
fragment I_ 
    : 'i' 
    | 'I'
    ;
    
fragment J_ 
    : 'j' 
    | 'J'
    ;
    
fragment K_ 
    : 'k' 
    | 'K'
    ;
    
fragment L_
    : 'l' 
    | 'L'
    ;
    
fragment M_
    : 'm' 
    | 'M'
    ;
    
fragment N_
    : 'n' 
    | 'N'
    ;
    
fragment O_ 
    : 'o' 
    | 'O'
    ;
    
fragment P_ 
    : 'p' 
    | 'P'
    ;
    
fragment Q_ 
    : 'q' 
    | 'Q'
    ;
    
fragment R_ 
    : 'r' 
    | 'R'
    ;
    
fragment S_ 
    : 's' 
    | 'S'
    ;
    
fragment T_ 
    : 't' 
    | 'T'
    ;
    
fragment U_
    : 'u' 
    | 'U'
    ;
    
fragment V_ 
    : 'v' 
    | 'V'
    ;
    
fragment W_ 
    : 'w' 
    | 'W'
    ;

fragment X_ 
    : 'x' 
    | 'X'
    ;
    
fragment Y_ 
    : 'y' 
    | 'Y'
    ;
    
fragment Z_ 
    : 'z' 
    | 'Z'
    ;

AND
    : A_ N_ D_ 
    ;
    
BETWEEN
    : B_ E_ T_ W_ E_ E_ N_ 
    ;    
    
BYTE
    : B_ Y_ T_ E_ 
    ;
    
CREATE
    : C_ R_ E_ A_ T_ E_ 
    ;
    
DATABASE
    : D_ A_ T_ A_ B_ A_ S_ E_ 
    ;
    
DECIMAL
    : D_ E_ C_ I_ M_ A_ L_ 
    ;
    
FROM
    : F_ R_ O_ M_ 
    ;
    
IN
    : I_ N_ 
    ;

INTO
    : I_ N_ T_ O_
    ;
    
INSERT
    : I_ N_ S_ E_ R_ T_
    ;    
    
INTEGER
    : I_ N_ T_ E_ G_ E_ R_ 
    ;
    
LONG
    : L_ O_ N_ G_ 
    ;   

MICROSECONDS
    : M_ I_ C_ R_ O_ S_ E_ C_ O_ N_ D_ S_ 
    ;

MICROSECONDS_TIMESTAMP
    : MICROSECONDS'_' T_ I_ M_ E_ S_ T_ A_ M_ P_
    ;

MILLISECONDS
    : M_ I_ L_ L_ I_ S_ E_ C_ O_ N_ D_ S_ 
    ;

MILLISECONDS_TIMESTAMP
    : MILLISECONDS'_' T_ I_ M_ E_ S_ T_ A_ M_ P_
    ;

NANOSECONDS
    : N_ A_ N_ O_ S_ E_ C_ O_ N_ D_ S_ 
    ;

NANOSECONDS_TIMESTAMP
    : NANOSECONDS'_' T_ I_ M_ E_ S_ T_ A_ M_ P_
    ;
    
NOT
    : N_ O_ T_
    ;    
    
OR
    : O_ R_
    ;       

SECONDS
    : S_ E_ C_ O_ N_ D_ S_ 
    ;

SECONDS_TIMESTAMP
    : SECONDS'_' T_ I_ M_ E_ S_ T_ A_ M_ P_
    ;

SELECT
    : S_ E_ L_ E_ C_ T_ 
    ;

TIMESERIES
    : T_ I_ M_ E_ S_ E_ R_ I_ E_ S_
    ;

TIME_UNIT
    : T_ I_ M_ E_'_' U_ N_ I_ T_
    ;

TIMEZONE
    : T_ I_ M_ E_ Z_ O_ N_ E_
    ;

USE
    : U_ S_ E_ 
    ;

VALUES
    : V_ A_ L_ U_ E_ S_;

WHERE
    : W_ H_ E_ R_ E_ 
    ;

statements
    : (statement ';')* 
    ;

statement
    : select 
    | insert
    | useDatabase
    | createTimeSeries 
    | createDatabase
    ;

createDatabase
    : CREATE DATABASE ID 
    ;

createTimeSeries
    : CREATE TIMESERIES ID '(' recordsDefinition ')' timeSeriesOptions 
    ;

recordsDefinition
    : recordDefinition (',' recordDefinition)* 
    ;

recordDefinition
    : ID '(' fieldsDefinition ')' 
    ;

fieldsDefinition
    : fieldDefinition (',' fieldDefinition)* 
    ;

fieldDefinition
    : ID type
    ;

timeSeriesOptions
    : (timeSeriesOption)* 
    ;

timeSeriesOption
    : TIMEZONE '=' STRING 
    | TIME_UNIT '=' timeUnit 
    ;
                 
timeUnit
    : NANOSECONDS 
    | MICROSECONDS 
    | MILLISECONDS 
    | SECONDS
    ;
    
type
    : BYTE 
    | INTEGER 
    | LONG 
    | DECIMAL 
    | NANOSECONDS_TIMESTAMP 
    | MICROSECONDS_TIMESTAMP 
    | MILLISECONDS_TIMESTAMP 
    | SECONDS_TIMESTAMP
    ; 
      
useDatabase
    : USE ID
    ;

insert
    : INSERT INTO recordName ('(' fieldList ')')? VALUES '(' valueList ')' 
    ;
    
recordName
    : ID'.'ID 
    ;    

fieldList
    : ID (',' ID )* 
    ;    
    
valueList
    : value (',' value )*
    ;    
    
select
    : SELECT '*' FROM ID (whereClause)?
    ;

whereClause
    : WHERE predicate
    ;
     
predicate
    : '('predicate')'
    | predicate AND predicate
    | predicate OR predicate
    | inPredicate
    | betweenPredicate
    | simplePredicate
    ;

inPredicate 
    : ID NOT? IN '(' value (',' value )* ')'
    ;
    
betweenPredicate 
    : ID NOT? BETWEEN value AND value
    ;    
    
simplePredicate
    : ID operator value
    ;

operator
    : '=' 
    | '>=' 
    | '>' 
    | '<=' 
    | '<' 
    | '!='
    ; 

value
    : STRING
    | timeValue
    | NUMBER
    ;     

timeValue
    : NUMBER ('s' | 'ms' | 'µs' | 'ns') 
    | '\'' DATE (TIME)? '\'' 
    ;
    
DATE 
    : '0'..'3' '0'..'9' '-' '0'..'1' '0'..'9' '-' '0'..'9' '0'..'9' '0'..'9' '0'..'9'
    ;    
    
TIME 
    : '0'..'2' '0'..'9' ':' '0'..'5' '0'..'9' ':' '0'..'5' '0'..'9' '.' ('0'..'9' '0'..'9' '0'..'9')?
    ;     
    
ID
    : ID_LETTER (ID_LETTER | DIGIT)* 
    ;

fragment ID_LETTER
    : 'a'..'z'
    |'A'..'Z'
    |'_' 
    ;

fragment DIGIT 
    : '0'..'9' 
    ;

NUMBER 
    : '0'..'9' ('0'..'9')* ('.' '0'..'9' ('0'..'9')*)* ('E' '-'* '0'..'9' ('0'..'9')*)* 
    ;
    
STRING
    : '\'' .*?'\'' 
    ; 
    
WS
    : [ \t\r\n]+ -> skip 
    ;