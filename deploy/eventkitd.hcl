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
        image = "img.dev.storj.io/nightly/eventkitd:latest"
        args  = [
          "/go/bin/eventkitd"
        ]
        network_mode = "host"
      }
    }
  }
}
