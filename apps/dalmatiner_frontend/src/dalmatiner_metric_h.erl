-module(dalmatiner_metric_h).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).

-ignore_xref([init/3, handle/2, terminate/3]).

init(_Transport, Req, []) ->
    {ok, Req, undefined}.

-dialyzer({no_opaque, handle/2}).
handle(Req, State) ->
    {ContentType, Req1} = dalmatiner_idx_handler:content_type(Req),
    case ContentType of
        html ->
            F = fun (Socket, Transport) ->
                        File = code:priv_dir(dalmatiner_frontend) ++
                            "/static/metric.html",
                        Transport:sendfile(Socket, File)
                end,
            Req2 = cowboy_req:set_resp_body_fun(F, Req1),
            {ok, Req3} = cowboy_req:reply(200, Req2),
            {ok, Req3, State};
        _ ->
            {Collection, Req2} = cowboy_req:binding(collection, Req1),
            {Prefix, Req3} = cowboy_req:binding(prefix, Req2),
            {Qs, Req4} = cowboy_req:qs_vals(Req3),
            list_metrics(ContentType, Collection, Prefix, Qs, Req4, State)
    end.

terminate(_Reason, _Req, _State) ->
    ok.

list_metrics(ContentType, Collection, undefined, [], Req, State) ->
    {ok, Ms} = dqe_idx:metrics(Collection),
    list_metrics(ContentType, Ms, Req, State);
list_metrics(ContentType, Collection, Prefix64, Qs, Req, State) ->
    Prefix = decode_prefix(Prefix64),
    {Depth, Tags} = decode_qs(Qs),
    {ok, Ms} = dqe_idx:metrics(Collection, Prefix, Tags, Depth),
    Ms1 = [Prefix ++ M || M <- Ms],
    list_metrics(ContentType, Ms1, Req, State).

list_metrics(ContentType, Metrics, Req, State) ->
    Ms = [[{key, base64:encode(dproto:metric_from_list(M))},
           {parts, M}] || M <- Metrics],
    dalmatiner_idx_handler:send(ContentType, Ms, Req, State).

decode_qs(Qs) ->
    decode_qs(Qs, {1, []}).
decode_qs([], Acc) ->
    Acc;
decode_qs([ {<<"depth">>, undefined} | Rest ], Acc) ->
    decode_qs(Rest, Acc);
decode_qs([ {<<"depth">>, V} | Rest ], {_D, Tags}) when is_binary(V) ->
    Acc1 = {list_to_integer(binary_to_list(V)), Tags},
    decode_qs(Rest, Acc1);
decode_qs([ {Name, V} | Rest ], {D, Tags}) when is_binary(V) ->
    Tag = decode_tag(Name, V),
    decode_qs(Rest, {D, [Tag | Tags]} ).

decode_tag(Name, V) ->
    decode_tag(Name, V, <<>>).
decode_tag(<<>>, V, Acc) ->
    {<<>>, Acc, decode_value(V)};
decode_tag(<<":", R/binary>>, V, Acc) ->
    {Acc, R, decode_value(V)};
decode_tag(<<C, R/binary>>, V, Acc) ->
    decode_tag(R, V, <<Acc/binary, C>>).

decode_value(undefined) ->
    <<>>;
decode_value(V) ->
    V.

decode_prefix(undefined) ->
    [];
decode_prefix(<<>>) ->
    [];
decode_prefix(Prefix64) when is_binary(Prefix64) ->
    PrefixBin = base64:decode(Prefix64),
    dproto:metric_to_list(PrefixBin).
