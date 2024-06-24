export PYTHONPATH=/root/blocksight/BlockSightV3
hypercorn -b 127.0.0.1:5000 ./api/run_server:app

