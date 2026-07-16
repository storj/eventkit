job "eventkitd" {
  type        = "service"
  datacenters = ["dc1"]

  group "eventkitd" {

    count = 1

    task "daemon" {
      resources {
        memory = 500
        cpu    = 500
      }
      
      driver = "docker"
      config {
        image = "ghcr.io/storj/eventkit/eventkitd:20260716-7122e9e"
        network_mode = "host"
      }
    }
  }
}
