Как будет проходить любой запрос:

Client

 identity, определит caller

  ratelimit ограничит RPS

  возможно ещё надо ограничить количество одновременно открытых Upload/Download

   metrics start : count, duration, grpc code, active streams

    logging start : method, caller, duration, grpc status

     RPC handler

     logging finish

      metrics finish