resource "google_cloud_run_v2_job" "default" {
  provider = google-beta
  name     = "cloud-billing-forecasting-bfyex"
  project = "g-sql-morphic-luminous"
  location = "us-central1"
  launch_stage = "BETA"
 

 
  template {
    template {
      containers {
                    image = "gcr.io/g-sql-morphic-luminous/cloud-billing-forecasting-bfyez"
                    env {
                          name  = "KEY1"
                          value = "us-central1"
                        }
                }
            }
          }
}
