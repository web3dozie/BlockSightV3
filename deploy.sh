cd ..
rm -rf ./BlockSightV3Deploy
cp -r ./BlockSightV3 ./BlockSightV3Deploy
cd ./BlockSightV3Deploy
rm -rf venv
rm -rf .git
cd ..
tar -czf bs.tar.gz ./BlockSightV3Deploy
scp ./bs.tar.gz root@62.171.171.121:/root/blocksight
rm -rf ./BlockSightV3Deploy