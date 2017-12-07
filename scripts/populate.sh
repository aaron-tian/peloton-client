if [ "$#" -ne 2 ]; then
  echo "Please input parameters."
  echo "Example: "$0" with_index scale_factor"
  echo "The table size will be scale_factor * 1000."
  exit 1;
fi
# callgrind_control -i on
if [ "$1" -ne 0 ]; then
  ../driver_main -y populate -i -k $2$  
else
  ../driver_main -y populate -k $2$
fi
# callgrind_control -i off