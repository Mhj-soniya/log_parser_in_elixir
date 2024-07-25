defmodule LogParser.Parser do
  @moduledoc """
  This module parse the log file and saves the entries into the database table logs
  """

  require Logger
  alias Postgrex, as: Repo

  def start_link do# to connect elixir code to the postgres database
    config = [
      hostname: "localhost",
      username: "postgres",
      password: "postgres",
      database: "log_parser_db",
      pool_size: 10
    ] #postgrex.start_link/1 accepts keyword.t i.e. keywordlist

    case Repo.start_link(config) do
      {:ok, pid} ->
        {:ok, pid}
      {:error, reason} ->
        Logger.error("Failed to start the connection: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def import(file_path, pid) do
    File.stream!(file_path)
      |> Enum.each(fn line ->
        # IO.inspect(line);
        # IO.inspect(parse_content(line))
          case parse_content(line) do
            {:ok, logdata} -> insert(logdata, pid)
          end
      end)
    # case File.read(file) do
    #   {:ok, content} ->
    #     content
    #     |> parse_content
    #   {:error, _} -> :error
    # end
  end

  defp parse_content(content) do
    content_list = content
                |> String.split(" ", parts: 13)
    if length(content_list) == 13 do
      [address, ip, _ , _ , datetime, time_zone, method, path, http_version, status, size, referer, user_agent] = content_list
      datetime_str = datetime<>" "<>time_zone

      log_data = %{
        address: address,
        ip: ip,
        datetime: parse_datetime(datetime_str),
        method: String.trim(method, "\""),
        path: path,
        http_version: String.trim(http_version, "\""),
        status: String.to_integer(status),
        size: String.to_integer(size),
        referer: String.trim(referer, "\""),
        user_agent: user_agent |> String.replace("\n","") |> String.trim("\"")
      }
      {:ok, log_data}
    else
      [address, ip, _ , _ , datetime, time_zone, _ , status, size, referer, user_agent] = content_list
      datetime_str = datetime<>" "<>time_zone

      log_data = %{
        address: address,
        ip: ip,
        datetime: parse_datetime(datetime_str),
        method: nil,
        path: nil,
        http_version: nil,
        status: String.to_integer(status),
        size: String.to_integer(size),
        referer: String.trim(referer, "\""),
        user_agent: user_agent |> String.replace("\n","") |> String.trim("\"")
      }
      {:ok, log_data}
    end
  end

  def insert(log_data, pid) do
    sql = """
    INSERT INTO logs (address, ip, datetime, method, path, http_version, status, size, referer, user_agent)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    """

    params = [
      log_data.address,
      log_data.ip,
      log_data.datetime,
      log_data.method,
      log_data.path,
      log_data.http_version,
      log_data.status,
      log_data.size,
      log_data.referer,
      log_data.user_agent
    ]

    case Repo.query(pid, sql, params) do
      {:ok, _result} -> :ok
      {:error, reason} -> Logger.error("Failed to insert log: #{inspect(reason)}")
    end
  end

  def fetch_log(query, pid) do
    # sql = "SELECT * FROM logs LIMIT 10"

    case Repo.query(pid, query, []) do
      {:ok, logs} ->
        Logger.info("All logs successfully fetched");
        display(logs)
      {:error, reason} ->
        Logger.error("Failed to fetech logs: #{inspect(reason)}")
    end
  end

  def fetch_get_logs(pid) do
    sql = """
      SELECT address, ip, datetime, method, http_version, status, size FROM logs where method = 'GET'
    """

    case Repo.query(pid, sql, []) do
      {:ok, logs} ->
        # Logger.info("All logs successfully fetched");
        display(logs)
      {:error, reason} ->
        Logger.error("Failed to fetech logs: #{inspect(reason)}")
    end
  end

  defp display(logs) do

    IO.puts(Enum.join(logs.columns, " \t| "))
    IO.puts("_____________________________________________________________________________________________")
    for log <- logs.rows do
        log_str = Enum.join(log, " | ")
        IO.puts(log_str)
    end
  end

  def delete(pid) do
    sql = "DELETE FROM logs"

    case Repo.query(pid, sql, []) do
      {:ok, _result} ->
        Logger.info("All logs deleted successfully")
        :ok
      {:error, reason} -> Logger.error("Failed to insert log: #{inspect(reason)}")
    end
  end

  # to convert the datetime string
  defp parse_datetime(datetime_str) do
    datetime_str = String.slice(datetime_str, 1, 26) # Remove the brackets
    case Timex.parse(datetime_str, "{0D}/{Mshort}/{YYYY}:{h24}:{m}:{s} {Z}") do
      {:ok, datetime} -> datetime
      {:error, reason} ->
        Logger.error("Failed to parse datetime: #{datetime_str}, reason: #{inspect(reason)}")
        nil
    end
  end
end
