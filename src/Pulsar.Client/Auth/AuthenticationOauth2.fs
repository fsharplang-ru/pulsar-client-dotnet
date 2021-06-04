﻿module Pulsar.Client.Auth.Oauth2

open System
open System.Text.Json
open System.Net.Http
open System.Text.Json.Serialization
open Pulsar.Client.Api
open Pulsar.Client.Auth.Metadata
open Pulsar.Client.Auth.Token

type Credentials ={  

    [<JsonPropertyName("type")>]
    credsType : string
    
    [<JsonPropertyName("client_id")>]
    clientId : string
    
    [<JsonPropertyName("client_secret")>]
    clientSecret : string 

    [<JsonPropertyName("client_email")>]
    clientEmail : string 

    [<JsonPropertyName("issuer_url")>]
    issuerUrl : string 
}

//Gets a well-known metadata URL for the given OAuth issuer URL.
//https://tools.ietf.org/id/draft-ietf-oauth-discovery-08.html#ASConfig
let getWellKnownMetadataUrl (issuerUrl: Uri) : Uri =
    Uri(
        issuerUrl.AbsoluteUri
        + ".well-known/openid-configuration"
    )

let getMetaData (issuerUrl: Uri) : Async<Metadata> =
    async {
        use client = new HttpClient()
        let metadataDataUrl = getWellKnownMetadataUrl issuerUrl
        
        let! response =
            client.GetStringAsync metadataDataUrl
            |> Async.AwaitTask
        return JsonSerializer.Deserialize<Metadata> response        
    }

let createClient issuerUrl =

    let data =
        getMetaData issuerUrl |> Async.RunSynchronously

    let tokenUrl = data.tokenEndpoint
    TokenClient(Uri(tokenUrl))



let openAndDeserializeCreds uri =
    let text = System.IO.File.ReadAllText uri    
    JsonSerializer.Deserialize<Credentials>(text)
    

type AuthenticationOauth2(issuerUrl: Uri, credentials: Uri, audience: Uri) =
    inherit Authentication()
    let tokenClient  = createClient issuerUrl
    let credentials = openAndDeserializeCreds(credentials.LocalPath) 
    let mutable _token : Option<TokenResult * DateTime> = None   

    //https://datatracker.ietf.org/doc/html/rfc6749#section-4.2.2
    let isTokenExpiredOrEmpty () : bool =
        match _token with
        | Some v ->
            let tokenDuration =                
                TimeSpan.FromSeconds(float (fst v).expiresIn)

            let tokenExpiration = (snd v).Add tokenDuration
            DateTime.Now > tokenExpiration
        | _ -> true

    member x.token
        with get () = _token
        and set value = _token <- value

    
    //member x.credentials : Credentials = openAndDeserializeCreds(credentials.ToString())  
    override this.GetAuthMethodName() = "token"
    override this.GetAuthData() =
        let returnTokenAsProvider () =
            AuthenticationDataToken(fun () -> (fst _token.Value).accessToken) :> AuthenticationDataProvider

        match isTokenExpiredOrEmpty () with
        | true ->
            let newToken =
                    tokenClient.exchangeClientCredentials (
                        credentials.clientId,
                        credentials.clientSecret,
                        audience
                    )

            match newToken with
            | Result (v, d) ->
                this.token <- Some(v, d)
                returnTokenAsProvider ()
            | OauthError e ->
                raise (
                    TokenExchangeException(
                        e.error
                        + Environment.NewLine
                        + e.errorDescription
                        + Environment.NewLine
                        + e.errorUri
                    )
                )
            | OtherError e -> failwith e


        | false -> returnTokenAsProvider ()
