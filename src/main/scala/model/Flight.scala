package model

case class Flight(
    route_id:String="NA",
    year :Int,
    month :Int,
    dayofmonth :Int,
    dayofweek :Int,
    deptime :Int, 
    crsdeptime :Int, 
    arrtime :Int, 
    crsarrtime :Int, 
    uniquecarrier :String,
    flightnum :String, 
    tailnum :String, 
    actualelapsedtime :Int,
    crselapsedtime :Int, 
    airtime :Int,
    arrdelay :Int, 
    depdelay :Int, 
    origin :String, 
    dest :String, 
    distance :Int, 
    taxiin :String,
    taxiout :String,
    cancelled :String, 
    cancellationcode :String, 
    diverted :String, 
    carrierdelay :Int,
    weatherdelay :Int, 
    nasdelay :Int, 
    securitydelay :Int, 
    lateaircraftdelay :Int )