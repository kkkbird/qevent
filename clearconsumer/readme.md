# clearconsumer

This application is used to clear consumers of redis stream.

If we use hostname(pod name) as consumer name in k8s deployments, pod name will always different after new deployment and may not be used forevent, we can use this application to delete consumer whose idle time exceed MAX.
