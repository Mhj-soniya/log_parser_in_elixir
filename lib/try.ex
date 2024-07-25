defmodule Try do
  def import(file) do
    case File.read(file) do
      {:ok, content} ->
        content
        |> parse_content
      {:error, _} -> :error
    end
  end

  def parse_content(content) do
    content_list = content
                |> String.split(" ", parts: 13)
    [_address, _ip, _ , _ , datetime_str, time_zone, _method, _path, _http_version, _status, _size, _referer, _user_agent] = content_list
    datetime_str<>time_zone
  end
end
