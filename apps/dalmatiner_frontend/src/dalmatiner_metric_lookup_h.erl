-module(dalmatiner_metric_lookup_h).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).

-ignore_xref([init/3, handle/2, terminate/3]).

init(_Transport, Req, []) ->
    {ok, Req, undefined}.

-dialyzer({no_opaque, handle/2}).
handle(Req, State) ->
    {ContentType, Req1} = dalmatiner_idx_handler:content_type(Req),
    {Collection, Req2} = cowboy_req:binding(collection, Req1),
    {Tags, Req3} = cowboy_req:qs_val(<<"tags">>, Req2),
    %% TODO: cleanup debug statements
    lager:info("Recived metrics lookup query: ~p", [Tags]),
    Query = metrics_query_by_tags(Collection, [<<"all">>]),
    lookup_metrics(ContentType, Query, Req3, State).

terminate(_Reason, _Req, _State) ->
    ok.

%%
%% Internals
%%

metrics_query_by_tags(Collection, Tags) ->
     TagHStore = {[{<<"label:", T/binary>>, <<>>} || T <- Tags]},
     metrics_query(Collection, {"dimensions @> $1", [TagHStore]}).

%% lookup_metrics_by_source(ContentType, Collection, Source, Req, State) ->
%%     Prefix = decode_prefix(Prefix64),
%%     Depth = decode_depth(DepthBin),
%%     {ok, Ms} = dqe_idx:metrics(Collection, Prefix, Depth),
%%     Ms1 = [Prefix ++ M || M <- Ms],
%%     list_metrics(ContentType, Ms1, Req, State).

metrics_query(Collection, Condition) ->
    {CondExpression, CondValues} = Condition,
    Pos = length(CondValues),
    Expression = ["SELECT DISTINCT slice(dimensions, '{ddb:part_2, ddb:part_3, "
                  "ddb:part_4, ddb:part_5, ddb:part_6, ddb:part_7, ddb:part_8, "
                  "ddb:part_9, ddb:part_10, ddb:part_11, ddb:part_12, "
                  "ddb:part_13, ddb:part_14, ddb:part_15, ddb:part_16, "
                  "ddb:part_17, ddb:part_18, ddb:part_19, ddb:part_20, "
                  "ddb:part_21, ddb:part_22}')"
                  " FROM metrics"
                  " WHERE collection = $",
                  integer_to_binary(Pos + 1),
                  " AND ", CondExpression],
    {Expression, CondValues ++ [Collection]}.

lookup_metrics(ContentType, {Expression, Values}, Req, State) ->
    %% I put massive timeout here, because it can take ages for big accounts,
    %% across all tags
    case pgapp:equery(Expression, Values, 600000) of
        {ok, _Cols, Rows} ->
            Data = [encode_metric(M) || {M} <- Rows],
            dalmatiner_idx_handler:send(ContentType, Data, Req, State);
        Error ->
            lager:error("Error in metric lookup query [~s]: ~p", 
                        [Expression, Error]),
            cowboy_req:reply(500, Req)
    end.

%% It will encode metric in a format that is compatible with old dataloop api,
%% so we can just forward it in the app straight to console.
encode_metric({Dimensions}) ->
    PosPart = [{Pos, Part} || {<<"ddb:part_", Pos/binary>>, Part} <- Dimensions],
    MetricParts = [Part || {_Pos, Part} <- lists:sort(PosPart)],
    Metric = dproto:metric_from_list(MetricParts),
    #{id => base64:encode(Metric),
      name => dproto:metric_to_string(Metric, <<".">>)}.
