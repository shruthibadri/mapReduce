task :default => 'all'

task :all => ['MaxElev.jar', 'EastWest.jar']

task :clean do
  sh "rm *.class *.jar"
end

rule '.jar' => ['.class'] do |t|
  x = t.source.sub(/\.class$/, '*.class')
  sh "jar cf #{t.name} #{x}"
end

rule '.class' => ['.java'] do |t|
  sh "hadoop com.sun.tools.javac.Main #{t.source}"
end
