
## Clusters

gcloud dataproc clusters create cluster-project-prediction --enable-component-gateway --region us-south1 --subnet default --master-machine-type n2-standard-2 --master-boot-disk-type pd-balanced --master-boot-disk-size 100 --num-workers 3 --worker-machine-type n2-standard-2 --worker-boot-disk-type pd-balanced --worker-boot-disk-size 100 --image-version 2.2-debian12 --properties spark:spark.dataproc.enhanced.optimizer.enabled=true,spark:spark.dataproc.enhanced.execution.enabled=true --optional-components JUPYTER --project unir-predictivo-andina-espana

### Creando cluster 2 

gcloud dataproc clusters create cluster-hight-memory --enable-component-gateway --region us-south1 --master-machine-type n2-highmem-2 --master-boot-disk-type pd-balanced --master-boot-disk-size 200 --num-workers 2 --worker-machine-type n2-standard-16 --worker-boot-disk-type pd-balanced --worker-boot-disk-size 200 --image-version 2.2-debian12 --optional-components JUPYTER --project unir-predictivo-andina-espana

## Sincronizar archivos

gsutil -m cp -r C:\Users\USUARIO\Downloads\gcp\BK_20241209\0_transient\data-factory-0-transient-zone\* gs://data-factory-0-transient-zone/


## Listar archivos

gsutil ls -r gs://data-factory-1-raw-data-zone/ | sed 's|gs://data-factory-1-raw-data-zone/||' | awk '
{
    n = split($0, parts, "/");
    path = "";
    is_file = ($0 ~ /\.[a-zA-Z0-9]+$/);  # Detectar si es un archivo (basado en extensión)

    for (i = 1; i <= n; i++) {
        path = path parts[i];
        if (i < n || is_file) {  # Solo considerar carpetas si contienen algo o archivos
            if (!(path in seen)) {
                indent = "";
                for (j = 1; j < i; j++) {
                    indent = indent "    ";
                }
                print indent "|-- " parts[i];
                seen[path] = 1;
            }
        }
        path = path "/";
    }
}'


