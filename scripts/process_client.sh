if [ "$#" -ne 5 ]; then
  echo "Please input parameters."
  echo "Example: "$0" with_prepared_statement scale_factor operation_count update_ratio zipf_theta"
  echo "The table size will be scale_factor * 1000."
  exit 1;
fi
# callgrind_control -i on
if [ "$1" -ne 0 ]; then
  ../driver_main -y client -p -k $2$ -o $3$ -u $4$ -z $5$
else
  ../driver_main -y client -k $2$ -o $3$ -u $4$ -z $5$
fi
# callgrind_control -i off