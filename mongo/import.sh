#! /bin/bash
#Import script for the binary .json files to the mongo db container
FILES="/mongo-seed/*.json"
prefix="/mongo-seed/"
suffix=".json"
logfilename="logfile.txt"
log="$prefix$logfilename"

echo " /xxxxxx                                               /xx                        " | tee -a $log
echo "|_  xx_/                                              | xx                        " | tee -a $log
echo "  | xx   /xxxxxx/xxxx   /xxxxxx   /xxxxxx   /xxxxxx  /xxxxxx    /xxxxxx   /xxxxxx " | tee -a $log
echo "  | xx  | xx_  xx_  xx /xx__  xx /xx__  xx /xx__  xx|_  xx_/   /xx__  xx /xx__  xx" | tee -a $log
echo "  | xx  | xx \ xx \ xx| xx  \ xx| xx  \ xx| xx  \__/  | xx    | xxxxxxxx| xx  \__/" | tee -a $log
echo "  | xx  | xx | xx | xx| xx  | xx| xx  | xx| xx        | xx /xx| xx_____/| xx      " | tee -a $log
echo " /xxxxxx| xx | xx | xx| xxxxxx$/|  xxxxxx/| xx        |  xxxx/|  xxxxxxx| xx      " | tee -a $log
echo "|______/|__/ |__/ |__/| xx____/  \______/ |__/         \___/   \_______/|__/      " | tee -a $log
echo "                      | xx                                                        " | tee -a $log
echo "                      | xx                                                        " | tee -a $log
echo "                      |__/                                                        " | tee -a $log

echo "Trying to import to MongoDB..." | tee -a $log

#Iterating through the .json files in /out directory (monuted as /mongo-seed inside the docker container) and importing them to mongoDB.
for f in $FILES
do
    filename=${f#"$prefix"}
    echo "Importing ${filename}." | tee -a $log
    collectionname=${filename%"$suffix"}
    {
        mongoimport --host mongodb --db mongodb --collection $collectionname --type json --file /mongo-seed/$filename --jsonArray
    } 1>/dev/null 2>&1
    
    echo "${filename} Import successfull." | tee -a $log
    rm $f
    echo "${filename} binary file deleted successfully." | tee -a $log
    echo "" | tee -a $log
done
echo "Import to MongoDB done." | tee -a $log