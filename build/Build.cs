using System.IO;
using Nuke.Common;
using Nuke.Common.IO;
using Nuke.Common.Tools.DotNet;

class Build : NukeBuild
{
	public static int Main() => Execute<Build>(x => x.Package);
	
	readonly string version = "2.0";

	AbsolutePath BuildOutputDirectory => RootDirectory / "build-output";

	readonly (string name, string runtimeId, bool selfContained)[] publishRuntimes =
	{
		("win-x64", "win-x64", false),
		("linux-x64", "linux-x64", false),
		("linux-x64-selfcontained", "linux-x64", true)
	};

	Target Clean => _ => _
		.Executes(() =>
		{
			DotNetTasks.DotNetClean(x => x
				.SetProject("./Hayden.sln"));
		});

	void DoPublish(string prefix, string projectPath, bool package)
	{
		foreach (var runtime in publishRuntimes)
		{
			Serilog.Log.Information($"Publishing {prefix} {runtime.name}");

			var prefixFolder = BuildOutputDirectory / prefix;
			var outputFolder = prefixFolder / runtime.name;
			var outputZip = BuildOutputDirectory / $"{prefix}-{version}-{runtime.name}.zip";

			outputFolder.CreateOrCleanDirectory();

			DotNetTasks.DotNetPublish(x => x
				.SetProject(projectPath)
				.SetOutput(outputFolder)
				.SetConfiguration("Release")
				.SetRuntime(runtime.runtimeId)
				.SetPublishSingleFile(true)
				.SetPublishTrimmed(runtime.selfContained)
				.SetSelfContained(runtime.selfContained));

			if (package)
			{
				if (File.Exists(outputZip))
					outputZip.DeleteFile();

				outputFolder.CompressTo(outputZip);
				prefixFolder.DeleteDirectory();
			}
		}
	}

	Target PrePackage => _ => _
		.Before(Package)
		.Executes(() =>
		{
			BuildOutputDirectory.CreateOrCleanDirectory();
		});

	Target Package => _ => _
		.DependsOn(PrePackage)
		.Executes(() =>
		{
			BuildOutputDirectory.CreateDirectory();
			DoPublish("mysqlchump", "./mysqlchump/mysqlchump.csproj", true);
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