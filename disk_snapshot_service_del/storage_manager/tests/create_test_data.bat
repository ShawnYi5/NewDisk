REM will CLEAN test_data.json !!!!!!

pause

cd ..
cd ..
py -3 manage.py dumpdata storage_manager --indent 2 -o storage_manager\tests\test_data.json
pause
