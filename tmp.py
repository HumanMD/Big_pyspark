for trace in streaming_ev_object:
    V, W = ExtractInstanceGraph(trace, cr)
    # print("\n\n------------------------------------\nUnrepaired Instance Graph")
    # viewInstanceGraph(V, W)
    D, I = checkTraceConformance2(trace, net, initial_marking, final_marking)
    print(D)
    print(I)
    #todo !!
    if len(D) + len(I) > 0:
        Wi = irregularGraphReparing(V, W, D, I, cr)
    num = trace.attributes.get('concept:name')
