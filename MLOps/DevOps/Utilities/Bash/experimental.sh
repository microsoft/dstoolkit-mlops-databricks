echo "Bool Git"
echo $(CheckInfraGitPathChange.enableInfraDeploy)

echo "Source Branch Name."
echo "$(Build.SourceBranchName)"

echo "System.PR.SourceBranch"
echo "$(System.PullRequest.SourceBranch)"

echo "Build.SourceBranch"
echo "$(Build.SourceBranch)"


echo "The URL for the triggering repository"
echo "$(Build.Repository.Uri)"

echo "Build Reason"
echo "$(Build.Reason)"

echo "Build Source Version"
echo "$(Build.SourceVersion)"

echo "Build.SourceVersionMessage"
echo "$(Build.SourceVersion)"

echo "Build.Repository.Git.SubmoduleCheckout"
echo "$(Build.Repository.Git.SubmoduleCheckout)"

echo "System.PullRequest.TargetBranch"
echo "$(System.PullRequest.TargetBranch)"

echo "System.PullRequest.SourceBranch"
echo "$(System.PullRequest.SourceBranch)"