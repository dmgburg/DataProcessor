# Data Translator

## Configuration

To configure data translator you should provide 2 mappings via com.dmgburg.test.Configuration.Builder class:
1) Input column name -> Internal column name
2) input row id -> Intenal row id

To get mappings from files method com.dmgburg.test.Configuration.parse can be used.

## Translator 
Class com.dmgburg.test.DataTranslator contains parsing and translating logic of application. See javadoc for constructor parameters description

To process file call com.dmgburg.test.DataTranslator.parse with target file as parameter.