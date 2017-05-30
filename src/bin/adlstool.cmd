@ECHO OFF
SET "TOOLPATH=%~dp0"
java -cp %TOOLPATH%;%TOOLPATH%\* com.microsoft.azure.datalake.store.AdlsTool %*

