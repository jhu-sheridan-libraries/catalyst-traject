require 'traject/mock_writer'

settings do |variable|
  store "writer_class_name", "Traject::MockWriter"
  provide "log.batch_progress", 3000
end