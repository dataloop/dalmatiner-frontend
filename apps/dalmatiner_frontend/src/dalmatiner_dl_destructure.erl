-module(dalmatiner_dl_destructure).

-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).

-ifdef(TEST).
-export([destructure/1]).
-endif.

-include("dalmatiner_dl_query_model.hrl").

-ignore_xref([init/3, handle/2, terminate/3]).

init(_Transport, Req, []) ->
    {ok, Req, undefined}.

-dialyzer({no_opaque, handle/2}).
handle(Req, State) ->
    Req0 = cowboy_req:set_resp_header(<<"access-control-allow-origin">>,
                                      <<"*">>, Req),
    case cowboy_req:qs_val(<<"q">>, Req0) of
        {undefined, Req1} ->
            {ok, Req2} = cowboy_req:reply(
                           400,
                           [{<<"content-type">>, <<"text/plain">>}],
                           "Missing required q= parameter",
                           Req1),
            {ok, Req2, State};
        {Q, Req1} ->
            {ok, Query} = destructure(Q),
            {CType, Req2} = dalmatiner_idx_handler:content_type(Req1),
            dalmatiner_idx_handler:send(CType, Query, Req2, State)
    end.

terminate(_Reason, _Req, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

-spec destructure(binary()) -> {ok, query()} | badmatch | term().
destructure(Query) when is_binary(Query) ->
    case expand_query(Query) of
        {ok, ExpandedQ} ->
            lager:info("Expanded query: ~p~n", [ExpandedQ]),
            R = destructure_select(ExpandedQ),
            {ok, R};
        {error, {badmatch, _}} ->
            badmatch;
        {error, E} ->
            E
    end.

-spec expand_query(binary()) -> {ok, map()} | {error, term()}.
expand_query(Q) ->
    S = binary_to_list(Q),
    try
        {ok, L, _} = dql_lexer:string(S),
        dql_parser:parse(L)
    catch
        error:Reason ->
            {error, Reason}
    end.

-spec destructure_select(map()) -> query().
destructure_select({ select, SubSelects, [], T }) ->
    Query0 = maps:merge(timeframe(T), #{ parts => [] }),

    lists:foldl(fun (SubS, Acc = #{ parts := Parts }) ->
                        P = destructure_part(SubS, #{} ),
                        Acc#{ parts := [Parts] ++ [P] }
                end, Query0, SubSelects).

-spec destructure_part(map(), part()) -> part().
destructure_part(#{ op := get, args := [B, M] }, P) ->
    Selector = #{ bucket => B, metric => M },
    P#{ selector => Selector };

destructure_part(#{ op := sget, args := [B, M] }, P) ->
    Selector = #{ bucket => B, metric => M },
    P#{ selector => Selector };

destructure_part(#{ op := lookup, args := [B, undefined] }, P) ->
    Selector = #{ collection => B, metric => [<<"ALL">>] },
    P#{ selector => Selector };

destructure_part(#{ op := lookup, args := [B, undefined, Where] }, P) ->
    Condition = destructure_where(Where),
    Selector = #{ collection => B, metric => [<<"ALL">>],
                  condition => Condition },
    P#{ selector => Selector };

destructure_part(#{ op := lookup, args := [B, M] }, P) ->
    Selector = #{ collection => B, metric => M },
    P#{ selector => Selector };

destructure_part(#{ op := lookup, args := [B, M, Where] }, P) ->
    Condition = destructure_where(Where),
    Selector = #{ collection => B, metric => M, condition => Condition },
    P#{ selector => Selector };

%% Functions applied to selectors
destructure_part(#{ op := fcall,
                    args := #{ name := Name,
                               inputs := [#{op := Op} = H | R] }}, P)
  when Op =:= get; Op =:= sget; Op =:= lookup ->
    P1 = destructure_part(H, P),
    Args = destructure_arg(R),
    Fn = #{ name => Name, args => [selector | Args] },
    P1#{ fn => Fn };
destructure_part(#{ op := fcall,
                    args := #{ name := Name,
                               inputs := [#{op := fcall} = H | R] }}, P) ->
    P1 = #{ fn := InnerFn } = destructure_part(H, P),
    Args = destructure_arg(R),
    Fn = #{ name => Name, args => [InnerFn | Args] },
    P1#{ fn => Fn };
destructure_part(#{ op := fcall,
                    args := #{ name := Name,
                               inputs := [#{op := timeshift} = H | R] }}, P) ->
    P1 = destructure_part(H, P),
    Args = destructure_arg(R),
    Fn = #{ name => Name, args => [timeshift | Args] },
    P1#{ fn => Fn };

destructure_part(#{ op := timeshift,
                    args := [T, #{op := Op} = Q ] }, P)
  when Op =:= get; Op =:= sget; Op =:= lookup ->
    P1 = #{selector := S} = destructure_part(Q, P),
    S1 = S#{ timeshift => destructure_arg(T) },
    P1#{ selector := S1 };

%% -spec destructure_(map()) -> alias().
destructure_part(#{ op := named, args := [L, Q] }, P) when is_list(L) ->
    N = destructure_named(L),
    P1 = destructure_part(Q, P),
    Alias = #{ label => N },
    P1#{ alias => Alias }.

destructure_arg(L) when is_list(L) ->
    [destructure_arg(L1) || L1 <- L];

destructure_arg(N) when is_integer(N) ->
    N;
destructure_arg(N) when is_float(N) ->
    N;
destructure_arg({time, T, U}) ->
    Us = atom_to_binary(U, utf8),
    <<(integer_to_binary(T))/binary, " ", Us/binary>>;
destructure_arg(#{op := time, args := [T, U]}) ->
    Us = atom_to_binary(U, utf8),
    <<(integer_to_binary(T))/binary, " ", Us/binary>>;
destructure_arg(now) ->
    now.

-spec timeframe(map()) -> timeframe().
timeframe(#{ op := 'last', args := [Q] }) ->
    #{ m => abs, duration => destructure_arg(Q) };

timeframe(#{ op := 'between', args := [#{ op := 'ago', args := [T] }, B] }) ->
    #{ m => rel, beginning => destructure_arg(T),
       ending => destructure_arg(B) };

timeframe(#{ op := 'between', args := [A, B] }) ->
    #{ m => abs, beginning => destructure_arg(A),
       ending => destructure_arg(B) };

timeframe(#{ op := 'after', args := [A, B] }) ->
    #{ m => abs, beginning => destructure_arg(A),
       duration => destructure_arg(B) };

timeframe(#{ op := 'before', args := [#{ op := 'ago', args := [T] }, B] }) ->
    #{ m => rel, beginning => destructure_arg(T),
       duration => destructure_arg(B) };

timeframe(#{ op := 'before', args := [A, B] }) ->
    #{ m => abs, ending => destructure_arg(A),
       duration => destructure_arg(B) };

timeframe(#{ op := 'ago', args := [T] }) ->
    #{ m => rel, beginning => destructure_arg(T) }.

-spec deconstruct_tag({tag, binary(), binary()}) -> list(binary()).
deconstruct_tag({tag, <<>>, K}) ->
    [<<>>, K];
deconstruct_tag({tag, N, K}) ->
    [N, K].

destructure_where({'=', T, <<>>}) ->
    #{ op => 'present', args => [deconstruct_tag(T), <<>>] };
destructure_where({'=', T, V}) ->
    #{ op => 'eq', args => [deconstruct_tag(T), V] };
destructure_where({'!=', T, V}) ->
    #{ op => 'neq', args => [deconstruct_tag(T), V] };
destructure_where({'or', Clause1, Clause2}) ->
    P1 = destructure_where(Clause1),
    P2 = destructure_where(Clause2),
    #{ op => 'or', args => [ P1, P2 ] };
destructure_where({'and', Clause1, Clause2}) ->
    P1 = destructure_where(Clause1),
    P2 = destructure_where(Clause2),
    #{ op => 'and', args => [ P1, P2 ] }.

destructure_named(Ms) ->
    Ms1 = [destructure_name(E) || E <- Ms],
    <<".", Result/binary>> = destructure_named(Ms1, <<>>),
    Result.
destructure_named([Named | R], Acc) ->
    destructure_named(R, <<Acc/binary, ".", Named/binary, "">>);
destructure_named([], Acc) ->
    Acc.

destructure_name(B) when is_binary(B) ->
    <<"'", B/binary, "'">>;
destructure_name({pvar, I}) ->
    <<"$", (integer_to_binary(I))/binary>>;
destructure_name({dvar, {<<>>, K}}) ->
    <<"$'", K/binary, "'">>;
destructure_name({dvar, {Ns, K}}) ->
    <<"$'", Ns/binary, "':'", K/binary, "'">>.
