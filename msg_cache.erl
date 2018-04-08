-module(msg_cache).
-export([start_cache_workers/1,worker_loop/1,worker_start/1,list_cache_workers/0,main_loop/2,push/1,pop/0,call/2]).


-record(workerinfo,{workername,
	                workpid,
	                bufferlen = 0}).

-record(workerstate,{name,length=0,buffer = []}).

get_main_loop_name() ->
	mainloop.

get_worker_pidlist(Dic) ->
	Func = fun(_K,V,AccIn) ->
		[V#workerinfo.workpid | AccIn] end,
	lists:reverse(maps:fold(Func,[],Dic)).

get_worker_infolist(Dic) ->
	Func = fun(_K,V,AccIn) ->
		[{V#workerinfo.workername,V#workerinfo.workpid,V#workerinfo.bufferlen} | AccIn] end,
	lists:reverse(maps:fold(Func,[],Dic)).

%% start cache manager process
start_cache_workers(Names) ->
	Dic = start_workers(Names,#{},1),
	PPid = spawn(msg_cache,main_loop,[Dic,0]),
	register(mainloop,PPid),
	get_worker_pidlist(Dic).

%% we can add a param for counting push message , in case of error happened.
main_loop(Dic,MsgNum) ->
	receive
		{list_workers,From} -> 
			Worklist = get_worker_infolist(Dic),
			From ! Worklist,
			main_loop(Dic,MsgNum);
		{push,Msg,From} ->
			Key = (MsgNum rem maps:size(Dic)) + 1,
			V1workinforecord = maps:get(Key,Dic),
			(V1workinforecord#workerinfo.workpid) ! {push,Msg,From},
			Len = V1workinforecord#workerinfo.bufferlen,
			V2workinforecord = V1workinforecord#workerinfo{bufferlen = Len +1},
			%From ! ok,
			main_loop(Dic#{Key := V2workinforecord}, MsgNum + 1);
		{pop,From} ->
			if 
				MsgNum =< 0 ->
					From !{error,{poperror,empty}},
					main_loop(Dic,MsgNum);
				true ->
					TmpKey = (MsgNum rem maps:size(Dic)),
					if
						(TmpKey > 0) ->
							Key = TmpKey;
						true ->
							Key = maps:size(Dic)
					end,
					V1workinforecord = maps:get(Key,Dic),
					(V1workinforecord#workerinfo.workpid) ! {pop,From},
					Len = V1workinforecord#workerinfo.bufferlen,
					V2workinforecord = V1workinforecord#workerinfo{bufferlen = Len - 1},
					main_loop(Dic#{Key := V2workinforecord}, MsgNum-1)
			end;
		_Unsupported ->
			erlang:error(io_libs:format("mainloop unsupported msg: ",[_Unsupported]))
	end.

%% Functions can be call by erlang shell
list_cache_workers() ->
	call(get_main_loop_name(),{list_workers,self()}).

push(Msg) ->
	call(get_main_loop_name(),{push,Msg,self()}).

pop() ->
	call(get_main_loop_name(),{pop,self()}).

call(Pid, Request) ->
  Pid ! Request,
  receive
    Response -> Response
  after 3000 ->
    {error, api_timeout}
  end.

%% start a cache worker
start_workers([H|T],Dic,Num) ->
	Pid = worker_start([H]),
	Info = #workerinfo{workername = H, workpid = Pid},
	Dic1 = Dic#{Num => Info},
	start_workers(T,Dic1,Num+1);
start_workers([],Dic,_Num) ->
	Dic.

worker_start(Name) ->
	Pid = spawn(msg_cache,worker_loop,[#workerstate{name = Name}]),
	Pid.

worker_loop(State = #workerstate{length = Len, buffer = Buff}) ->
	receive
		{push,Msg,From} ->
			From ! ok,
			worker_loop(State#workerstate{length = Len + 1, buffer = [Msg | Buff]});
		{pop,From} ->
			case Buff of 
				[] ->
					From ! {error,{worker_poperror,empty}},
					worker_loop(State);
				[ToMsg | Msg] ->
					From ! ToMsg,
					worker_loop(State#workerstate{length = Len - 1,buffer = Msg})
			end;			
		_Unsupported ->
			erlang:error(io_libs:format("unsupported msg: ",[_Unsupported])),
			worker_loop(State)
	end.
