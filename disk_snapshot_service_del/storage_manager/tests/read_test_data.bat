REM will CLEAN DB !!!!!!

pause

cd ..
cd ..
py -3 manage.py loaddata storage_manager\tests\test_data.json
pause
