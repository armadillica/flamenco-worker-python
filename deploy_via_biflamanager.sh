#!/usr/bin/env bash

ssh -o ClearAllForwardings=yes biflamanager -T <<EOT
set -e
cd \$HOME/flamenco-worker

git reset --hard
git pull
pipenv install --dev --deploy
pipenv run ./mkdistfile.py

last_file=\$(ls -rt dist/flamenco-worker* | tail -n 1)
dirname=\$(echo \$last_file | sed s/-linux.*//)
tar_path=\$(pwd)/\$last_file

echo
echo "--------------------------------------------------------------"
echo "Deploying \$last_file"
echo "--------------------------------------------------------------"

cd /shared/bin/flamenco-worker
tar zxvf \$tar_path
rm -f flamenco-worker
ln -s \$dirname/flamenco-worker .

echo
echo "--------------------------------------------------------------"
echo "Done! Now restart workers to pick up the changes."
echo "--------------------------------------------------------------"
EOT
