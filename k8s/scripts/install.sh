echo "Welcome to UDiTE. The cluster is being installed."
printf "\n\n"

cd ..
#minikube start

for dir in */;
do
  if [ "$dir" = "scripts/" ]; then
    continue
  fi

  cd "$dir" || exit
  for entry in "."/00-*.yaml
    do
      kubectl apply -f "$entry"
    done
  cd ..
done