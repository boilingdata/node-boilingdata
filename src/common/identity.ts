import {
  CognitoIdToken,
  CognitoUserPool,
  CognitoUser,
  AuthenticationDetails,
  ChallengeName,
} from "amazon-cognito-identity-js";
import { fromCognitoIdentityPool } from "@aws-sdk/credential-provider-cognito-identity";
import { CognitoIdentityClient } from "@aws-sdk/client-cognito-identity";
import { getSignedWssUrl } from "./signature";
import { BDAWSRegion } from "boilingdata/boilingdata";

const IDP_REGION = "eu-west-1";
const UserPoolId = "eu-west-1_0GLV9KO1p";
const Logins = `cognito-idp.${IDP_REGION}.amazonaws.com/${UserPoolId}`;
const IdentityPoolId = "eu-west-1:bce21571-e3a6-47a4-8032-fd015213405f";
const poolData = { UserPoolId, ClientId: "6timr8knllr4frovfvq8r2o6oo" };
const Pool = new CognitoUserPool(poolData);

export interface BDCredentials {
  cognitoUsername: string;
  signedWebsocketUrl: string;
}

function getIdToken(Username: string, Password: string, mfa?: number, logger?: Console): Promise<CognitoIdToken> {
  return new Promise((resolve, reject) => {
    try {
      const loginDetails = { Username, Password };
      logger?.debug("====", Pool.getUserPoolId(), Pool.getClientId(), Pool.getCurrentUser());
      const userData = { Username, Pool };
      const cognitoUser = new CognitoUser(userData);
      logger?.debug("====", cognitoUser.getUsername());
      const authenticationDetails = new AuthenticationDetails(loginDetails);
      cognitoUser.authenticateUser(authenticationDetails, {
        mfaRequired: async function (challengeName: ChallengeName, challengeParameters: any) {
          logger?.debug({ callback: "mfaRequired", challengeName, challengeParameters });
          if (!mfa) return reject("MFA required");
          cognitoUser.sendMFACode(`${mfa}`, this);
        },
        totpRequired: async function (challengeName: ChallengeName, challengeParameters: any) {
          logger?.debug({ callback: "totpRequired", challengeName, challengeParameters });
          if (!mfa) return reject("TOTP required");
          cognitoUser.sendMFACode(`${mfa}`, this, "SOFTWARE_TOKEN_MFA");
        },
        onSuccess: (result: any) => resolve(result?.getIdToken(undefined, undefined, undefined, logger)),
        onFailure: (err: any) => reject(err),
      });
    } catch (err) {
      console.error(err);
      reject(err);
    }
  });
}

async function refreshCredsWithToken(jwtIdToken: string): Promise<any> {
  const cognitoidentity = new CognitoIdentityClient({
    credentials: fromCognitoIdentityPool({
      client: new CognitoIdentityClient({ region: IDP_REGION }),
      identityPoolId: IdentityPoolId,
      logins: {
        [Logins]: jwtIdToken,
      },
    }),
  });
  const creds = await cognitoidentity.config.credentials();
  // console.log(creds.expiration);
  return creds;
}

function getEndpointUrlPath(endpointUrl: string): string {
  return new URL(endpointUrl).pathname;
}

function getWsApiDomain(region: string, endpointUrl?: string): string {
  return endpointUrl ? new URL(endpointUrl).host : `${region}.api.boilingdata.com`;
  // return `api.boilingdata.com`;
}

export async function getBoilingDataCredentials(
  username?: string,
  password?: string,
  region: BDAWSRegion = IDP_REGION,
  endpointUrl?: string,
  mfa?: number,
  authContext?: { idToken?: any; accessToken?: any; refreshToken?: any },
  logger?: Console,
): Promise<BDCredentials> {
  const webSocketHost = getWsApiDomain(region, endpointUrl);
  let idToken: CognitoIdToken | undefined = undefined;
  try {
    if (!authContext && username && password) {
      logger?.debug("Fetching ID token with username and pw");
      idToken = await getIdToken(username, password, mfa, logger);
    } else if (authContext && authContext.idToken?.jwtToken) {
      logger?.debug("Using existing ID token");
      idToken = new CognitoIdToken({ IdToken: authContext.idToken?.jwtToken });
    }
    if (!idToken) throw new Error("No credentials for creating signed WS URL");
    const creds = await refreshCredsWithToken(idToken.getJwtToken());
    const { accessKeyId, secretAccessKey, sessionToken } = creds;
    if (!accessKeyId || !secretAccessKey) throw new Error("Missing credentials (after refresh)!");
    const credentials = { accessKeyId, secretAccessKey, sessionToken };
    const signedWebsocketUrl = await getSignedWssUrl(
      webSocketHost,
      credentials,
      region,
      "wss",
      endpointUrl ? getEndpointUrlPath(endpointUrl) : "",
    );
    const cognitoUsername = idToken.decodePayload()["cognito:username"];
    return { cognitoUsername, signedWebsocketUrl };
  } catch (err) {
    console.error(err);
    throw err;
  }
}
