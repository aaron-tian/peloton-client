echo "disabled for now!"
exit 1;

if [ "$#" -ne 5 ]; then
  echo "Please input parameters."
  echo "Example: "$0" with_prepared_statement scale_factor operation_count update_ratio zipf_theta"
  echo "The table size will be scale_factor * 1000."
  exit 1;
fi
../peloton_client -y procedure -p $1$ -k $2$ -o $3$ -u $4$ -z $5$