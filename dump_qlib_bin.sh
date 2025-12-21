set -e
set -x
WORKING_DIR=${1} 
QLIB_REPO=${2:-https://github.com/microsoft/qlib.git} 

# 1. 安装 Dolt 工具（仅安装二进制程序，不涉及数据）
if ! command -v dolt &> /dev/null
then
    curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | bash
fi

# 2. 身份配置（防止启动报错）
dolt config --global --add user.email "action@github.com"
dolt config --global --add user.name "GitHub Action"

# 3. 创建“影子”数据目录（仅存储元数据，不存储表数据）
# 严格不在 WORKING_DIR 下下载大数据
PROXY_DATA_DIR="$HOME/dolt_metadata"
mkdir -p "$PROXY_DATA_DIR/investment_data"
cd "$PROXY_DATA_DIR/investment_data"

# 初始化一个空库并关联远程
dolt init
dolt remote add origin chenditc/investment_data
# 重点：fetch 只拉取索引/元数据，不下载数据文件
dolt fetch origin master
dolt checkout -f master || dolt checkout -b master origin/master

# 4. 启动 SQL Server
# --data-dir 指向我们刚才建立的“影子”目录
# -r (readonly) 确保不会因为意外写入产生本地文件
dolt sql-server --data-dir "$PROXY_DATA_DIR" --host 0.0.0.0 --port 3306 -r &


# 2. 准备工作目录（仅用于代码，不用于存储数据库）
mkdir -p "$WORKING_DIR"
cd "$WORKING_DIR"

# 3. 克隆 Qlib 代码库
if [ ! -d "$WORKING_DIR/qlib" ]; then
    git clone "$QLIB_REPO" "$WORKING_DIR/qlib"
fi


# 5. 等待 SQL Server 端口就绪
echo "Waiting for Dolt Remote Server to start..."
for i in {1..15}; do
    if nc -z localhost 3306; then
        echo "Dolt Remote Server is online."
        break
    fi
    sleep 2
done

# 6. 开始数据处理逻辑
# 假设你的代码库是在本地存在的（例如从 git 或其他地方拉取的 investment_data 代码包）
# 注意：此目录现在只存放 Python 脚本，不存放 .dolt 数据
cd "$WORKING_DIR/investment_data"

mkdir -p ./qlib/qlib_source

# 此时 Python 连接 localhost:3306，数据将从远程流式传输到 Python 进程
python3 ./qlib/dump_all_to_qlib_source.py

export PYTHONPATH=$PYTHONPATH:$WORKING_DIR/qlib/scripts
cd ./qlib
python3 ./normalize.py normalize_data --source_dir ./qlib_source/ --normalize_dir ./qlib_normalize --max_workers=16 --date_field_name="tradedate" 
python3 $WORKING_DIR/qlib/scripts/dump_bin.py dump_all --data_path ./qlib_normalize/ --qlib_dir $WORKING_DIR/qlib_bin --date_field_name=tradedate --exclude_fields=tradedate,symbol

mkdir -p ./qlib_index/
python3 ./dump_index_weight.py 

cd "$WORKING_DIR/investment_data"
python3 ./tushare/dump_day_calendar.py $WORKING_DIR/qlib_bin/

# 7. 任务完成，关闭 Dolt 后台进程，释放所有资源
killall dolt || true

# 8. 后续清理与打包
cp qlib/qlib_index/csi* $WORKING_DIR/qlib_bin/instruments/

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
