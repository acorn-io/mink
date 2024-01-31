# Mink is not Kubernetes

MINK provides the ability to leverage Kubernetes' Aggregated API Server feature to serve anything as a Kubernetes conformant API.
Currently Mink provides two backends: SQL (MySQL) or Kubernetes.
It also provides the tools to build translation layers e.g. to front actual Kubernetes objects.
Example: Acorn provides the "artificial" `App` resource (public), backed by the actual `AppInstance` resource (Kubernetes resource).
