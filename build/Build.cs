using System.IO;
using Nuke.Common;
using Nuke.Common.IO;
using Nuke.Common.Tooling;
using Nuke.Common.Tools.DotNet;
using Nuke.Common.Tools.Npm;
using static Nuke.Common.IO.FileSystemTasks;

class Build : NukeBuild
{
	public static int Main() => Execute<Build>(x => x.Package);
	
	readonly string version = "1.3";

	AbsolutePath BuildOutputDirectory => RootDirectory / "build-output";

	readonly string[] publishRuntimes =
	{
		"win-x64",
		"linux-x64",
		"linux-x64-bflat",
		"portable"
	};

	Target Clean => _ => _
		.Executes(() =>
		{
			DotNetTasks.DotNetClean(x => x
				.SetProject("./Hayden.sln"));
		});

	void DoPublish(string prefix, string projectPath, bool package)
	{
		foreach (string runtimeName in publishRuntimes)
		{
			Serilog.Log.Information($"Publishing {prefix} {runtimeName}");

			var prefixFolder = BuildOutputDirectory / prefix;
			var outputFolder = prefixFolder / runtimeName;
			var outputZip = BuildOutputDirectory / $"{prefix}-{version}-{runtimeName}.zip";

			
			EnsureCleanDirectory(outputFolder);

			if (runtimeName == "linux-x64-bflat")
			{
				//ProcessTasks.StartProcess(new BflatToolSettings())
			}
			else
			{
				DotNetTasks.DotNetPublish(x => x
					.SetProject(projectPath)
					.SetOutput(outputFolder)
					.SetConfiguration("Release")
					.SetRuntime(runtimeName != "portable" ? runtimeName : null)
					.SetSelfContained(runtimeName != "portable"));
			}

			if (package)
			{
				if (File.Exists(outputZip))
					DeleteFile(outputZip);

				CompressionTasks.CompressZip(outputFolder, outputZip);
				DeleteDirectory(prefixFolder);
			}
		}
	}

	Target PrePackage => _ => _
		.Before(Package)
		.Executes(() =>
		{
			EnsureCleanDirectory(BuildOutputDirectory);
		});

	Target Package => _ => _
		.DependsOn(PrePackage)
		.Executes(() =>
		{
			EnsureExistingDirectory(BuildOutputDirectory);
			DoPublish("hayden-cli", "./Hayden/Hayden.csproj", true);
		});
}

//internal class BflatToolSettings : ToolSettings
//{
//	public override string ProcessToolPath => "bflat";

//	protected override Arguments ConfigureProcessArguments(Arguments arguments)
//	{
//		return arguments.Add()
//	}
//}