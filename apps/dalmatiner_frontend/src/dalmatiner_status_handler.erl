-module(dalmatiner_status_handler).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).

-define(TIMEOUT, 30000).


init(_Transport, Req, []) ->
    {ok, Req, undefined}.

-dialyzer({no_opaque, handle/2}).
handle(Req, State) ->
    Req0 = cowboy_req:set_resp_header(
             <<"access-control-allow-origin">>, <<"*">>, Req),
    {ContentType, Req1} = dalmatiner_idx_handler:content_type(Req0),
    DDBStatus = check_ddb_status(),
    Data = #{dfe => ok, ddb_connection => DDBStatus},
    Status = case DDBStatus of
                 ok -> 200;
                 _ -> 500
             end,
    send(ContentType, Status, Data, Req1, State).

terminate(_Reason, _Req, _State) ->
    ok.

check_ddb_status() ->
    {Host, Port} = endpoint(),
    {ok, Socket} = gen_tcp:connect(Host, Port,
                                   [{active, false},
                                    {send_timeout, 5000}]),
    Status = case gen_tcp:send(Socket, <<"poke\n">>) of
                 ok -> ok;
                 {error, Reason} -> Reason
             end,
    ok = gen_tcp:close(Socket),
    Status.

endpoint() ->
    case application:get_env(ddb_connection, backend) of
        {ok, {Host, Port}} ->
            {Host, Port};
        _ ->
            {ok, Host} = application:get_env(ddb_connection,
                                             backend_host),
            {ok, Port} = application:get_env(ddb_connection,
                                             backend_port),
            {Host, Port}
    end.

send(json, Status, D, Req, State) ->
    {ok, Req1} =
        cowboy_req:reply(
          Status, [{<<"content-type">>, <<"application/json">>}],
          jsone:encode(D), Req),
    {ok, Req1, State};
send(msgpack, Status, D, Req, State) ->
    {ok, Req1} =
        cowboy_req:reply(
          Status, [{<<"content-type">>, <<"application/x-msgpack">>}],
          msgpack:pack(D, [jsx]), Req),
    {ok, Req1, State};
send(_, 200, _D, Req, State) ->
    {ok, Req1} =
        cowboy_req:reply(
          200, [{<<"content-type">>, <<"text/plain">>}],
          "dfe running\n", Req),
    {ok, Req1, State};
send(_, Status, _D, Req, State) ->
    {ok, Req1} = cowboy_req:reply(Status, Req),
    {ok, Req1, State}.
