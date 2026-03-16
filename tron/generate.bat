git submodule update --init --recursive
mkdir tron/generated
python -m grpc_tools.protoc -I .\tron\googleapis\ -I .\tron\protos\ --python_out=./tron/generated --grpc_python_out=./tron/generated .\tron\protos\api\*.proto
python -m grpc_tools.protoc -I .\tron\googleapis\ -I .\tron\protos\ --python_out=./tron/generated --grpc_python_out=./tron/generated .\tron\protos\core\*.proto