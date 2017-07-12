-module(dalmatiner_metric_lookup_h).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).

-ignore_xref([init/3, handle/2, terminate/3]).

-define(PG_TIMEOUT, 30000).                     % Maximum time in ms to wait
                                                % for postgres.
-define(WEEK_LENGTH, 604800).                   % A week in seconds
-define(WEEK_START, 1420070400).                % 2015-01-01 in seconds since
                                                % epoch. It aligns with weeks.

init(_Transport, Req, []) ->
    {ok, Req, undefined}.

-dialyzer({no_opaque, handle/2}).
handle(Req, State) ->
    {ContentType, Req1} = dalmatiner_idx_handler:content_type(Req),
    {Collection, Req2} = cowboy_req:binding(collection, Req1),
    {QVals, Req3} = cowboy_req:qs_vals(Req2),
    Dimensions = metrics_criteria(QVals, []),
    lager:debug("Looking up metrics with dimensions: ~p", [Dimensions]),
    Query = metrics_query(Collection, Dimensions),
    Pool = collection_pool(Collection),
    lookup_metrics(ContentType, Pool, Query, Req3, State).

terminate(_Reason, _Req, _State) ->
    ok.


%%
%% Internals
%%

metrics_criteria([], Acc) ->
    {[week_dimension() | Acc]};
metrics_criteria([{<<"tag">>, Tag} | Rest], Acc) ->
    Dim = {<<"label:", Tag/binary>>, <<>>},
    metrics_criteria(Rest, [Dim | Acc]);
metrics_criteria([{<<"source">>, Source} | Rest], Acc) ->
    Dim = {<<"dl:source">>, Source},
    metrics_criteria(Rest, [Dim | Acc]).

metrics_query(Collection, Dimensions) ->
    Expression = "SELECT DISTINCT slice(dimensions, '{ddb:part_2, ddb:part_3, "
        "ddb:part_4, ddb:part_5, ddb:part_6, ddb:part_7, ddb:part_8, "
        "ddb:part_9, ddb:part_10, ddb:part_11, ddb:part_12, "
        "ddb:part_13, ddb:part_14, ddb:part_15, ddb:part_16, "
        "ddb:part_17, ddb:part_18, ddb:part_19, ddb:part_20, "
        "ddb:part_21, ddb:part_22}')"
        " FROM metrics"
        " WHERE collection = $1"
        " AND dimensions @> $2",
    {Expression, [Collection, Dimensions]}.

collection_pool(Collection) ->
    case ets:lookup(dqe_idx_pg_poolmap, Collection) of
        [{_C, Pool}| _] ->
            Pool;
        [] ->
            default
    end.

%% TODO: when doing one level at a time, we will need to add flags saying
%% that it has children and whether it is selectable.

lookup_metrics(ContentType, Pool, {Expression, Values}, Req, State) ->
    %% I put massive timeout here, because it can take ages for big accounts,
    %% across all tags
    case pgapp:equery(Pool, Expression, Values, ?PG_TIMEOUT) of
        {ok, _Cols, Rows} ->
            Data = [encode_metric(M) || {M} <- Rows],
            dalmatiner_idx_handler:send(ContentType, Data, Req, State);
        {error, timeout} ->
            lager:error("PG metric lookup query timeout [~s]",
                        [Expression]),
            {ok, Req1} = cowboy_req:reply(504,
                                          [{<<"content-type">>,
                                            <<"text/plain">>}],
                                          "Query too wide! Could not get "
                                          "results in reasonable time", Req),
            {ok, Req1, State};
        Error ->
            lager:error("Error in metric lookup query [~s]: ~p",
                        [Expression, Error]),
            {ok, Req1} = cowboy_req:reply(500,
                                          [{<<"content-type">>,
                                            <<"text/plain">>}],
                                          "Internal server error", Req),
            {ok, Req1, State}
    end.

%% It will encode metric in a format that is compatible with old dataloop api,
%% so we can just forward it in the app straight to console.
encode_metric({Dims}) ->
    PosPart = [{Pos, Part} || {<<"ddb:part_", Pos/binary>>, Part} <- Dims],
    MetricParts = [Part || {_Pos, Part} <- lists:sort(PosPart)],
    Metric = dproto:metric_from_list(MetricParts),
    #{id => base64:encode(Metric),
      name => dproto:metric_to_string(Metric, <<".">>)}.

week_dimension() ->
    %% W add extra magin of 30 minutes to give indexer some extra time to
    %% build up indexes just after rolling to new week
    Time = erlang:system_time(seconds) - 1800,
    Week = trunc((Time - ?WEEK_START) / ?WEEK_LENGTH),
    WeekBin = integer_to_binary(Week),
    {<<"dl:week_", WeekBin/binary>>, <<"">>}.
