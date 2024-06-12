export PYTHONPATH=/root/blocksight/BlockSightV3
hypercorn -b 0.0.0.0:80 ./api/run_server:app 2>> error.log

