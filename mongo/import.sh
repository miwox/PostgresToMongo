#! /bin/bash

FILES="/mongo-seed/*.json"
prefix="/mongo-seed/"
suffix=".json"
echo "Trying to import to MongoDB..."
for f in $FILES
do
    filename=${f#"$prefix"}
    echo "Importing ${filename}."
    collectionname=${filename%"$suffix"}
    {
        mongoimport --host mongodb --db mongodb --collection $collectionname --type json --file /mongo-seed/$filename --jsonArray
    } 1>/dev/null 2>&1
    echo "${filename} Import successfull."
    echo ""
done
echo "Import done."