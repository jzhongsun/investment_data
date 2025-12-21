set -e
set -x
WORKING_DIR=${1} 
QLIB_REPO=${2:-https://github.com/microsoft/qlib.git} 

# 1. 安装 Dolt
if ! command -v dolt &> /dev/null
then
    # 2025年推荐做法：如果是自动化环境，可能需要 sudo
    curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | bash
fi

# 2. 核心调整：中转模式初始化（不下载全量数据）
# 创建并进入目标目录
mkdir -p "$WORKING_DIR/dolt/investment_data"
cd "$WORKING_DIR/dolt/investment_data"

# 如果没有 .dolt 目录，说明是首次运行，执行初始化
if [ ! -d ".dolt" ]; then
    dolt init
    dolt remote add origin chenditc/investment_data
fi

# 3. 克隆 Qlib 代码库
if [ ! -d "$WORKING_DIR/qlib" ]; then
    git clone "$QLIB_REPO" "$WORKING_DIR/qlib"
fi

# 4. 仅拉取元数据索引，确保能看到最新的分支状态
cd "$WORKING_DIR/dolt/investment_data"
dolt fetch origin master
# 确保本地有一个指向远程 master 的分支
dolt checkout -f master || dolt checkout -b master origin/master

# 5. 启动 SQL Server（按需拉取模式）
# 使用 & 符号后台运行，Dolt 将在查询时实时下载所需数据块
dolt sql-server --host 0.0.0.0 --port 3306 &

# 6. 等待 SQL Server 端口就绪
echo "Waiting for Dolt SQL Server to start..."
for i in {1..15}; do
    if nc -z localhost 3306; then
        echo "Dolt SQL Server is online."
        break
    fi
    sleep 2
done

# --- 以下保持你原有的逻辑，但修正了路径引用的连贯性 ---

# 注意：这里确保进入你 Python 脚本所在的目录
# 假设你的 dump 脚本在 $WORKING_DIR/dolt/investment_data 下
cd "$WORKING_DIR/dolt/investment_data"

mkdir -p ./qlib/qlib_source
# 执行 dump，此时 python 会连接本地 3306 端口，Dolt 会按需下载数据
python3 ./qlib/dump_all_to_qlib_source.py

export PYTHONPATH=$PYTHONPATH:$WORKING_DIR/qlib/scripts
cd ./qlib
python3 ./normalize.py normalize_data --source_dir ./qlib_source/ --normalize_dir ./qlib_normalize --max_workers=16 --date_field_name="tradedate" 
python3 $WORKING_DIR/qlib/scripts/dump_bin.py dump_all --data_path ./qlib_normalize/ --qlib_dir $WORKING_DIR/qlib_bin --date_field_name=tradedate --exclude_fields=tradedate,symbol

mkdir -p ./qlib_index/
python3 ./dump_index_weight.py 

cd "$WORKING_DIR/dolt/investment_data"
python3 ./tushare/dump_day_calendar.py $WORKING_DIR/qlib_bin/

# 任务完成，关闭 Dolt 后台进程
killall dolt || true

cp qlib/qlib_index/csi* $WORKING_DIR/qlib_bin/instruments/

# 打包输出
cd $WORKING_DIR
tar -czvf ./qlib_bin.tar.gz ./qlib_bin/
ls -lh ./qlib_bin.tar.gz

OUTPUT_DIR=${OUTPUT_DIR:-/output}
if [ -d "${OUTPUT_DIR}" ]; then
    mv ./qlib_bin.tar.gz "${OUTPUT_DIR}/"
    ls -lh "${OUTPUT_DIR}/qlib_bin.tar.gz"
else
    echo "Generated tarball at $WORKING_DIR/qlib_bin.tar.gz"
fi
