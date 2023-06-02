require 'snowflake'
require 'rufus-scheduler'
require 'odbc'

# Configuración de Snowflake
Snowflake.configure do |config|
  config.account = 'gerardo.zuniga-jimenez@shipt.com'
  config.username = 'gerardo.zuniga-jimenez'
  config.password = 'b9krfwp7!TPcQ6B'
  config.warehouse = 'shipt.us-east-1.snowflakecomputing.com'
  config.database = 'prd_datalakehouse'
  config.schema = 'PRD_BACKEND_ENGINEER_WHS'
  config.driver = 'ODBC'
  config.odbc_driver = '{Snowflake}'
end

# Definición de la consulta a ejecutar
QUERY = 'SELECT * FROM OG_VIEWS'

# Creación del scheduler
scheduler = Rufus::Scheduler.new

# Definición de la tarea programada
scheduler.every '1h' do
  # Conexión a Snowflake
  connection = Snowflake::Connection.new

  begin
    # Establecer la conexión ODBC
    odbc_connection = ODBC.connect(connection.odbc_string)

    # Ejecución de la consulta
    result = odbc_connection.run(QUERY)

    # Procesamiento de los resultados
    result.each do |row|
      # Realiza alguna operación con los datos obtenidos
      puts row
    end
  rescue ODBC::Error => e
    puts "Error al ejecutar la consulta: #{e.message}"
  ensure
    # Cierre de la conexión
    odbc_connection.disconnect if odbc_connection
  end
end

# Mantener el programa en ejecución
scheduler.join
