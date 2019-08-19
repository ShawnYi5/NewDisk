REM will CLEAN DB !!!!!!

pause

cd ..
cd ..
py -3 manage.py loaddata storage_manager\tests\test_data.json
py -3 manage.py shell -c "from storage_manager.visualization import run;run.run()"
pause