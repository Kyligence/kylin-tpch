#!/bin/bash

if [[ -z "$KYLIN_HOME" ]]; then
    echo "Please set KYLIN_HOME"
    exit 1
fi

echo "Checking hive database tpch_flat_orc_$1"
if hive -e "use tpch_flat_orc_$1" 2>/dev/null >/dev/null; then
    echo "Use Hive database 'tpch_flat_orc_$1'"
else
    echo "Hive database 'tpch_flat_orc_$1' does not exists!"
    exit 1
fi

echo "Creating views"
sed -i -e "s/use tpch_flat_orc_.*/use tpch_flat_orc_$1;/g" models/kylin-tpch-create-views.sql
hive -f models/kylin-tpch-create-views.sql

echo "Import Kylin project"
rm -rf models/tmp
cp -r models/metadata models/tmp
for f in models/tmp/table/*; do mv $f `echo $f | sed "s/TPCH_FLAT_ORC_2/TPCH_FLAT_ORC_$1/"`; done
find models/tmp -type f -name "*.json" -exec sed -i -e "s/TPCH_FLAT_ORC_2/TPCH_FLAT_ORC_$1/g" {} \;
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool upload models/tmp
