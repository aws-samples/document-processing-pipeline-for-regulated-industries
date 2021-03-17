echo "Copying lambda functions..."
rm -rf ../infrastructure/code/*
rsync -zavh ../code/ ../infrastructure/code/
cd ../infrastructure/code/lambda_layer/pipeline/python
pip3 install -r requirements.txt --target .
echo "Done building pipeline / registry layer!"
cd ../../metadata-services/python
pip3 install -r requirements.txt --target .
echo "Done building metadata services layer!"
