(* Lexicons - generated from atproto lexicons *)

module App = struct
  module Bsky = struct
    module Video = struct
      module UploadVideo = App_bsky_video_uploadVideo
      module Defs = App_bsky_video_defs
      module GetJobStatus = App_bsky_video_getJobStatus
      module GetUploadLimits = App_bsky_video_getUploadLimits
    end
    module Contact = struct
      module Defs = App_bsky_contact_defs
      module SendNotification = App_bsky_contact_sendNotification
      module GetSyncStatus = App_bsky_contact_getSyncStatus
      module StartPhoneVerification = App_bsky_contact_startPhoneVerification
      module GetMatches = App_bsky_contact_getMatches
      module ImportContacts = App_bsky_contact_importContacts
      module DismissMatch = App_bsky_contact_dismissMatch
      module RemoveData = App_bsky_contact_removeData
      module VerifyPhone = App_bsky_contact_verifyPhone
    end
    module Bookmark = struct
      module Defs = App_bsky_bookmark_defs
      module DeleteBookmark = App_bsky_bookmark_deleteBookmark
      module GetBookmarks = App_bsky_bookmark_getBookmarks
      module CreateBookmark = App_bsky_bookmark_createBookmark
    end
    module Embed = struct
      module Defs = App_bsky_embed_defs
      module Record = App_bsky_embed_record
      module Images = App_bsky_embed_images
      module RecordWithMedia = App_bsky_embed_recordWithMedia
      module Video = App_bsky_embed_video
      module External = App_bsky_embed_external
    end
    module Notification = struct
      module Defs = App_bsky_notification_defs
      module RegisterPush = App_bsky_notification_registerPush
      module PutPreferences = App_bsky_notification_putPreferences
      module PutActivitySubscription = App_bsky_notification_putActivitySubscription
      module Declaration = App_bsky_notification_declaration
      module PutPreferencesV2 = App_bsky_notification_putPreferencesV2
      module UpdateSeen = App_bsky_notification_updateSeen
      module ListActivitySubscriptions = App_bsky_notification_listActivitySubscriptions
      module UnregisterPush = App_bsky_notification_unregisterPush
      module GetPreferences = App_bsky_notification_getPreferences
      module ListNotifications = App_bsky_notification_listNotifications
      module GetUnreadCount = App_bsky_notification_getUnreadCount
    end
    module Unspecced = struct
      module GetSuggestedFeedsSkeleton = App_bsky_unspecced_getSuggestedFeedsSkeleton
      module SearchStarterPacksSkeleton = App_bsky_unspecced_searchStarterPacksSkeleton
      module Defs = App_bsky_unspecced_defs
      module GetOnboardingSuggestedStarterPacksSkeleton = App_bsky_unspecced_getOnboardingSuggestedStarterPacksSkeleton
      module GetSuggestedUsers = App_bsky_unspecced_getSuggestedUsers
      module GetPostThreadOtherV2 = App_bsky_unspecced_getPostThreadOtherV2
      module GetSuggestedStarterPacks = App_bsky_unspecced_getSuggestedStarterPacks
      module GetSuggestedStarterPacksSkeleton = App_bsky_unspecced_getSuggestedStarterPacksSkeleton
      module GetOnboardingSuggestedStarterPacks = App_bsky_unspecced_getOnboardingSuggestedStarterPacks
      module GetSuggestedUsersSkeleton = App_bsky_unspecced_getSuggestedUsersSkeleton
      module GetPostThreadV2 = App_bsky_unspecced_getPostThreadV2
      module GetTrends = App_bsky_unspecced_getTrends
      module SearchActorsSkeleton = App_bsky_unspecced_searchActorsSkeleton
      module GetSuggestionsSkeleton = App_bsky_unspecced_getSuggestionsSkeleton
      module SearchPostsSkeleton = App_bsky_unspecced_searchPostsSkeleton
      module GetAgeAssuranceState = App_bsky_unspecced_getAgeAssuranceState
      module GetPopularFeedGenerators = App_bsky_unspecced_getPopularFeedGenerators
      module InitAgeAssurance = App_bsky_unspecced_initAgeAssurance
      module GetTrendingTopics = App_bsky_unspecced_getTrendingTopics
      module GetTaggedSuggestions = App_bsky_unspecced_getTaggedSuggestions
      module GetSuggestedFeeds = App_bsky_unspecced_getSuggestedFeeds
      module GetTrendsSkeleton = App_bsky_unspecced_getTrendsSkeleton
      module GetConfig = App_bsky_unspecced_getConfig
    end
    module Graph = struct
      module GetStarterPacks = App_bsky_graph_getStarterPacks
      module GetSuggestedFollowsByActor = App_bsky_graph_getSuggestedFollowsByActor
      module Block = App_bsky_graph_block
      module GetStarterPacksWithMembership = App_bsky_graph_getStarterPacksWithMembership
      module Follow = App_bsky_graph_follow
      module Defs = App_bsky_graph_defs
      module GetListsWithMembership = App_bsky_graph_getListsWithMembership
      module UnmuteActorList = App_bsky_graph_unmuteActorList
      module GetListBlocks = App_bsky_graph_getListBlocks
      module Listblock = App_bsky_graph_listblock
      module GetStarterPack = App_bsky_graph_getStarterPack
      module Starterpack = App_bsky_graph_starterpack
      module MuteActorList = App_bsky_graph_muteActorList
      module MuteThread = App_bsky_graph_muteThread
      module SearchStarterPacks = App_bsky_graph_searchStarterPacks
      module GetActorStarterPacks = App_bsky_graph_getActorStarterPacks
      module GetLists = App_bsky_graph_getLists
      module GetFollowers = App_bsky_graph_getFollowers
      module UnmuteThread = App_bsky_graph_unmuteThread
      module MuteActor = App_bsky_graph_muteActor
      module GetMutes = App_bsky_graph_getMutes
      module Listitem = App_bsky_graph_listitem
      module List = App_bsky_graph_list
      module GetKnownFollowers = App_bsky_graph_getKnownFollowers
      module Verification = App_bsky_graph_verification
      module GetListMutes = App_bsky_graph_getListMutes
      module GetFollows = App_bsky_graph_getFollows
      module GetBlocks = App_bsky_graph_getBlocks
      module GetRelationships = App_bsky_graph_getRelationships
      module UnmuteActor = App_bsky_graph_unmuteActor
      module GetList = App_bsky_graph_getList
    end
    module Feed = struct
      module Generator = App_bsky_feed_generator
      module SendInteractions = App_bsky_feed_sendInteractions
      module Defs = App_bsky_feed_defs
      module GetFeedGenerators = App_bsky_feed_getFeedGenerators
      module GetTimeline = App_bsky_feed_getTimeline
      module GetFeedGenerator = App_bsky_feed_getFeedGenerator
      module GetAuthorFeed = App_bsky_feed_getAuthorFeed
      module GetLikes = App_bsky_feed_getLikes
      module Postgate = App_bsky_feed_postgate
      module Threadgate = App_bsky_feed_threadgate
      module GetPostThread = App_bsky_feed_getPostThread
      module GetActorLikes = App_bsky_feed_getActorLikes
      module Like = App_bsky_feed_like
      module GetRepostedBy = App_bsky_feed_getRepostedBy
      module Repost = App_bsky_feed_repost
      module DescribeFeedGenerator = App_bsky_feed_describeFeedGenerator
      module SearchPosts = App_bsky_feed_searchPosts
      module GetPosts = App_bsky_feed_getPosts
      module GetFeed = App_bsky_feed_getFeed
      module GetQuotes = App_bsky_feed_getQuotes
      module GetFeedSkeleton = App_bsky_feed_getFeedSkeleton
      module GetListFeed = App_bsky_feed_getListFeed
      module GetSuggestedFeeds = App_bsky_feed_getSuggestedFeeds
      module GetActorFeeds = App_bsky_feed_getActorFeeds
      module Post = App_bsky_feed_post
    end
    module Richtext = struct
      module Facet = App_bsky_richtext_facet
    end
    module Ageassurance = struct
      module Begin = App_bsky_ageassurance_begin
      module Defs = App_bsky_ageassurance_defs
      module GetState = App_bsky_ageassurance_getState
      module GetConfig = App_bsky_ageassurance_getConfig
    end
    module Actor = struct
      module SearchActorsTypeahead = App_bsky_actor_searchActorsTypeahead
      module Defs = App_bsky_actor_defs
      module PutPreferences = App_bsky_actor_putPreferences
      module GetProfile = App_bsky_actor_getProfile
      module GetSuggestions = App_bsky_actor_getSuggestions
      module SearchActors = App_bsky_actor_searchActors
      module GetProfiles = App_bsky_actor_getProfiles
      module Status = App_bsky_actor_status
      module GetPreferences = App_bsky_actor_getPreferences
      module Profile = App_bsky_actor_profile
    end
    module Labeler = struct
      module Defs = App_bsky_labeler_defs
      module Service = App_bsky_labeler_service
      module GetServices = App_bsky_labeler_getServices
    end
  end
end
module Com = struct
  module Atproto = struct
    module Temp = struct
      module DereferenceScope = Com_atproto_temp_dereferenceScope
      module AddReservedHandle = Com_atproto_temp_addReservedHandle
      module CheckSignupQueue = Com_atproto_temp_checkSignupQueue
      module CheckHandleAvailability = Com_atproto_temp_checkHandleAvailability
      module RequestPhoneVerification = Com_atproto_temp_requestPhoneVerification
      module RevokeAccountCredentials = Com_atproto_temp_revokeAccountCredentials
      module FetchLabels = Com_atproto_temp_fetchLabels
    end
    module Identity = struct
      module UpdateHandle = Com_atproto_identity_updateHandle
      module Defs = Com_atproto_identity_defs
      module SignPlcOperation = Com_atproto_identity_signPlcOperation
      module SubmitPlcOperation = Com_atproto_identity_submitPlcOperation
      module ResolveIdentity = Com_atproto_identity_resolveIdentity
      module RefreshIdentity = Com_atproto_identity_refreshIdentity
      module ResolveHandle = Com_atproto_identity_resolveHandle
      module RequestPlcOperationSignature = Com_atproto_identity_requestPlcOperationSignature
      module GetRecommendedDidCredentials = Com_atproto_identity_getRecommendedDidCredentials
      module ResolveDid = Com_atproto_identity_resolveDid
    end
    module Admin = struct
      module UpdateAccountEmail = Com_atproto_admin_updateAccountEmail
      module GetAccountInfo = Com_atproto_admin_getAccountInfo
      module GetSubjectStatus = Com_atproto_admin_getSubjectStatus
      module Defs = Com_atproto_admin_defs
      module SearchAccounts = Com_atproto_admin_searchAccounts
      module UpdateAccountPassword = Com_atproto_admin_updateAccountPassword
      module UpdateAccountHandle = Com_atproto_admin_updateAccountHandle
      module GetInviteCodes = Com_atproto_admin_getInviteCodes
      module UpdateAccountSigningKey = Com_atproto_admin_updateAccountSigningKey
      module EnableAccountInvites = Com_atproto_admin_enableAccountInvites
      module DisableAccountInvites = Com_atproto_admin_disableAccountInvites
      module DisableInviteCodes = Com_atproto_admin_disableInviteCodes
      module UpdateSubjectStatus = Com_atproto_admin_updateSubjectStatus
      module SendEmail = Com_atproto_admin_sendEmail
      module GetAccountInfos = Com_atproto_admin_getAccountInfos
      module DeleteAccount = Com_atproto_admin_deleteAccount
    end
    module Label = struct
      module Defs = Com_atproto_label_defs
      module QueryLabels = Com_atproto_label_queryLabels
    end
    module Server = struct
      module RequestEmailConfirmation = Com_atproto_server_requestEmailConfirmation
      module ReserveSigningKey = Com_atproto_server_reserveSigningKey
      module Defs = Com_atproto_server_defs
      module GetServiceAuth = Com_atproto_server_getServiceAuth
      module GetAccountInviteCodes = Com_atproto_server_getAccountInviteCodes
      module CreateSession = Com_atproto_server_createSession
      module ListAppPasswords = Com_atproto_server_listAppPasswords
      module CreateInviteCodes = Com_atproto_server_createInviteCodes
      module DeleteSession = Com_atproto_server_deleteSession
      module RevokeAppPassword = Com_atproto_server_revokeAppPassword
      module CreateAppPassword = Com_atproto_server_createAppPassword
      module ActivateAccount = Com_atproto_server_activateAccount
      module DescribeServer = Com_atproto_server_describeServer
      module ConfirmEmail = Com_atproto_server_confirmEmail
      module GetSession = Com_atproto_server_getSession
      module RefreshSession = Com_atproto_server_refreshSession
      module DeactivateAccount = Com_atproto_server_deactivateAccount
      module UpdateEmail = Com_atproto_server_updateEmail
      module ResetPassword = Com_atproto_server_resetPassword
      module CheckAccountStatus = Com_atproto_server_checkAccountStatus
      module RequestEmailUpdate = Com_atproto_server_requestEmailUpdate
      module RequestPasswordReset = Com_atproto_server_requestPasswordReset
      module RequestAccountDelete = Com_atproto_server_requestAccountDelete
      module CreateAccount = Com_atproto_server_createAccount
      module DeleteAccount = Com_atproto_server_deleteAccount
      module CreateInviteCode = Com_atproto_server_createInviteCode
    end
    module Lexicon = struct
      module ResolveLexicon = Com_atproto_lexicon_resolveLexicon
      module Schema = Com_atproto_lexicon_schema
    end
    module Sync = struct
      module GetHead = Com_atproto_sync_getHead
      module GetBlob = Com_atproto_sync_getBlob
      module GetRepo = Com_atproto_sync_getRepo
      module NotifyOfUpdate = Com_atproto_sync_notifyOfUpdate
      module Defs = Com_atproto_sync_defs
      module RequestCrawl = Com_atproto_sync_requestCrawl
      module ListBlobs = Com_atproto_sync_listBlobs
      module GetLatestCommit = Com_atproto_sync_getLatestCommit
      module GetRepoStatus = Com_atproto_sync_getRepoStatus
      module GetRecord = Com_atproto_sync_getRecord
      module ListHosts = Com_atproto_sync_listHosts
      module ListRepos = Com_atproto_sync_listRepos
      module GetHostStatus = Com_atproto_sync_getHostStatus
      module GetBlocks = Com_atproto_sync_getBlocks
      module ListReposByCollection = Com_atproto_sync_listReposByCollection
      module GetCheckout = Com_atproto_sync_getCheckout
    end
    module Repo = struct
      module StrongRef = Com_atproto_repo_strongRef
      module Defs = Com_atproto_repo_defs
      module ListMissingBlobs = Com_atproto_repo_listMissingBlobs
      module CreateRecord = Com_atproto_repo_createRecord
      module DeleteRecord = Com_atproto_repo_deleteRecord
      module PutRecord = Com_atproto_repo_putRecord
      module UploadBlob = Com_atproto_repo_uploadBlob
      module ImportRepo = Com_atproto_repo_importRepo
      module DescribeRepo = Com_atproto_repo_describeRepo
      module GetRecord = Com_atproto_repo_getRecord
      module ApplyWrites = Com_atproto_repo_applyWrites
      module ListRecords = Com_atproto_repo_listRecords
    end
    module Moderation = struct
      module Defs = Com_atproto_moderation_defs
      module CreateReport = Com_atproto_moderation_createReport
    end
  end
end
